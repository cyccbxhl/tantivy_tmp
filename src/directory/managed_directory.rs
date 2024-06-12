use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{io, result};
use sysinfo::{Pid, System};


use crate::core::UNCOMMITTED_FILEPATH;
use crate::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use crate::directory::footer::{Footer, FooterProxy};
use crate::directory::{
    DirectoryLock, FileHandle, FileSlice, GarbageCollectionResult, Lock, WatchCallback,
    WatchHandle, WritePtr, META_LOCK,
};
use crate::error::DataCorruption;
use crate::{Directory, SegmentId};

/// Returns true if the file is "managed".
/// Non-managed file are not subject to garbage collection.
///
/// Filenames that starts by a "." -typically locks-
/// are not managed.
fn is_managed(path: &Path) -> bool {
    match path.file_name().and_then(|name| name.to_str()) {
        Some(name) => !name.starts_with('.'),
        None => true
    }
}

/// Thanks to this list, it implements a `garbage_collect` method
/// that removes the files that were created by tantivy and are not
/// useful anymore.
#[derive(Debug)]
pub struct ManagedDirectory {
    directory: Box<dyn Directory>,
}

impl ManagedDirectory {
    /// Wraps a directory as managed directory.
    pub fn wrap(directory: Box<dyn Directory>) -> crate::Result<ManagedDirectory> {
        Ok(ManagedDirectory { directory })
    }

    /// Read the uncommitted segments from filepath
    pub fn read_uncommitted(&self) -> crate::Result<HashMap<SegmentId, u32>> {
        match self.directory.atomic_read(&UNCOMMITTED_FILEPATH) {
            Ok(data) => {
                let uncommitted_files_json = String::from_utf8_lossy(&data);
                let uncommitted_files: HashMap<SegmentId, u32> = serde_json::from_str(&uncommitted_files_json)
                    .map_err(|e| {
                        DataCorruption::new(
                            UNCOMMITTED_FILEPATH.to_path_buf(),
                            format!("Uncommitted file cannot be deserialized: {e:?}. "),
                        )
                    })?;
                Ok(uncommitted_files)
            }
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(HashMap::new()),
            io_err @ Err(OpenReadError::IoError { .. }) => Err(io_err.err().unwrap().into()),
            Err(OpenReadError::IncompatibleIndex(incompatibility)) => {
                // For the moment, this should never happen `meta.json`
                // do not have any footer and cannot detect incompatibility.
                Err(crate::TantivyError::IncompatibleIndex(incompatibility))
            }
        }
    }

    /// Write a new segment into uncommitted filepath
    pub fn write_uncommitted(
        &self,
        new_segment: SegmentId
    ) -> io::Result<()> {
        let pid = std::process::id();
        let mut uncommitted_segments = self.read_uncommitted().unwrap();
        uncommitted_segments.insert(new_segment, pid);
        self.save_uncommitted(&uncommitted_segments)
    }

    /// Save the uncommitted_segments into filepath
    pub fn save_uncommitted(
        &self,
        uncommitted_segments: &HashMap<SegmentId, u32>
    ) -> io::Result<()> {
        let mut w = serde_json::to_vec(&uncommitted_segments)?;
        writeln!(&mut w)?;
        self.directory.atomic_write(&UNCOMMITTED_FILEPATH, &w[..])?;
        Ok(())
    }

    /// Garbage collect unused files.
    ///
    /// Removes the files that were created by `tantivy` and are not
    /// used by any segment anymore.
    ///
    /// * `living_files` - List of files that are still used by the index.
    ///
    /// The use a callback ensures that the list of living_files is computed
    /// while we hold the lock on meta.
    ///
    /// This method does not panick nor returns errors.
    /// If a file cannot be deleted (for permission reasons for instance)
    /// an error is simply logged, and the file remains in the list of managed
    /// files.
    pub fn garbage_collect<L: FnOnce() -> HashSet<SegmentId>>(
        &mut self,
        get_committed_segments: L
    ) -> crate::Result<GarbageCollectionResult> {
        info!("Garbage collect");
        let mut files_to_delete = vec![];

        // It is crucial to get the living files after acquiring the
        // read lock of meta information. That way, we
        // avoid the following scenario.
        //
        // 1) we get the list of living files.
        // 2) someone creates a new file.
        // 3) we start garbage collection and remove this file
        // even though it is a living file.
        //
        // releasing the lock as .delete() will use it too.
        {
            // The point of this second "file" lock is to enforce the following scenario
            // 1) process B tries to load a new set of searcher.
            // The list of segments is loaded
            // 2) writer change meta.json (for instance after a merge or a commit)
            // 3) gc kicks in.
            // 4) gc removes a file that was useful for process B, before process B opened it.
            match self.acquire_lock(&META_LOCK) {
                Ok(_meta_lock) => {
                    let mut segments_to_delete: HashSet<SegmentId> = HashSet::new();
                    let mut living_uncommitted: HashMap<SegmentId, u32> = HashMap::new();
                    let uncommitted_segments = self.read_uncommitted().unwrap();
                    let committed_segments = get_committed_segments();
                    let original_len = uncommitted_segments.len();

                    // reload uncommitted segments to delete leftover ones
                    let mut system = System::new_all();
                    system.refresh_processes();
                    for (segment_id, pid) in uncommitted_segments {
                        if system.process(Pid::from(pid as usize)).is_some() {
                            living_uncommitted.insert(segment_id, pid);
                            continue;
                        }
                        if !committed_segments.contains(&segment_id) {
                            segments_to_delete.insert(segment_id);
                        }
                    }
                    if living_uncommitted.len() != original_len {
                        self.save_uncommitted(&living_uncommitted)
                            .expect("Save uncommitted segments failed");
                    }

                    for path in self.directory.get_files().unwrap() {
                        if !is_managed(&path) {
                            continue;
                        }
                        let seg_str = match path.file_stem().and_then(|s| s.to_str()) {
                            Some(s) => s,
                            None => continue,
                        };

                        if let Ok(segment_id) = SegmentId::from_uuid_string(seg_str) {
                            if segments_to_delete.contains(&segment_id) {
                                println!("Delete leftover uncommitted file {:?}", path);
                                files_to_delete.push(path);
                            }
                        }
                    }
                }
                Err(err) => {
                    error!("Failed to acquire lock for GC");
                    return Err(crate::TantivyError::from(err));
                }
            }
        }

        let mut failed_to_delete_files = vec![];
        let mut deleted_files = vec![];

        for file_to_delete in files_to_delete {
            match self.delete(&file_to_delete) {
                Ok(_) => {
                    info!("Deleted {:?}", file_to_delete);
                    deleted_files.push(file_to_delete);
                }
                Err(file_error) => {
                    match file_error {
                        DeleteError::FileDoesNotExist(_) => {
                            deleted_files.push(file_to_delete.clone());
                        }
                        DeleteError::IoError { .. } => {
                            failed_to_delete_files.push(file_to_delete.clone());
                            if !cfg!(target_os = "windows") {
                                // On windows, delete is expected to fail if the file
                                // is mmapped.
                                error!("Failed to delete {:?}", file_to_delete);
                            }
                        }
                    }
                }
            }
        }

        Ok(GarbageCollectionResult {
            deleted_files,
            failed_to_delete_files,
        })
    }
}

impl Directory for ManagedDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        let file_slice = self.directory.open_read(path)?;
        let (footer, reader) = Footer::extract_footer(file_slice)
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        footer.is_compatible()?;
        Ok(reader)
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        Ok(io::BufWriter::new(Box::new(FooterProxy::new(
            self.directory
                .open_write(path)?
                .into_inner()
                .map_err(|_| ())
                .expect("buffer should be empty"),
        ))))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.directory.atomic_write(path, data)
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        self.directory.atomic_read(path)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.directory.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.directory.exists(path)
    }

    fn get_files(&self) -> Result<HashSet<PathBuf>, OpenReadError> {
        self.directory.get_files()
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        self.directory.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> crate::Result<WatchHandle> {
        self.directory.watch(watch_callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.directory.sync_directory()?;
        Ok(())
    }
}

impl Clone for ManagedDirectory {
    fn clone(&self) -> ManagedDirectory {
        ManagedDirectory {
            directory: self.directory.box_clone(),
        }
    }
}

#[cfg(feature = "mmap")]
#[cfg(test)]
mod tests_mmap_specific {
    use std::path::PathBuf;

    use tempfile::TempDir;
    use crate::Index;
    use crate::indexer::LogMergePolicy;
    use crate::schema::{INDEXED, Schema};

    #[test]
    fn test_concurrent_gc() -> crate::Result<()> {
        let mut schema_builder1 = Schema::builder();
        let int_field1 = schema_builder1.add_u64_field("intval", INDEXED);
        let schema1 = schema_builder1.build();
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        let tempdir_path2 = tempdir_path.clone();
        Index::create_in_dir(&tempdir_path, schema1).unwrap();
        let index1 = Index::open_in_dir(tempdir_path).unwrap();

        let index2 = Index::open_in_dir(tempdir_path2).unwrap();
        let int_field2 = int_field1.clone();

        {
            let mut log_merge_policy = LogMergePolicy::default();
            log_merge_policy.set_min_num_segments(1);
            log_merge_policy.set_max_docs_before_merge(1);
            log_merge_policy.set_min_layer_size(0);
            let log_merge_policy2 = log_merge_policy.clone();

            let mut index_writer1 = index1.writer_for_pg(1, 150_000_000)?;
            index_writer1.set_merge_policy(Box::new(log_merge_policy));

            index_writer1.add_document(doc!(int_field1=>1_u64))?;
            index_writer1.add_document(doc!(int_field1=>2_u64))?;

            let mut index_writer2 = index2.writer_for_pg(1, 150_000_000)?;
            index_writer2.set_merge_policy(Box::new(log_merge_policy2));


            index_writer1.add_document(doc!(int_field1=>3_u64))?;
            index_writer2.add_document(doc!(int_field2=>4_u64))?;
            index_writer1.add_document(doc!(int_field1=>5_u64))?;
            index_writer2.add_document(doc!(int_field2=>6_u64))?;
            index_writer1.add_document(doc!(int_field1=>7_u64))?;
            index_writer2.add_document(doc!(int_field2=>8_u64))?;

            index_writer2.prepare_commit()?;

            index_writer2.garbage_collect_files();
            index_writer1.garbage_collect_files();

            index_writer2.concurrent_commit()?;
            index_writer2.consider_merge_options();
            // index_writer2.wait_merging_threads().unwrap();

            index_writer1.concurrent_commit()?;
            index_writer1.consider_merge_options();
            // index_writer1.wait_merging_threads().unwrap();
        }

        let _segment_ids = index1
            .searchable_segment_ids()
            .expect("Searchable segments failed.");

        let reader = index1.reader().unwrap();
        let searcher = reader.searcher();
        let segment_readers = searcher.segment_readers();
        let mut total_docs = 0;
        for segment in segment_readers {
            total_docs += segment.num_docs();
        }
        assert_eq!(total_docs, 8);
        Ok(())
    }
}
