use redb::{Database, Durability, ReadableDatabase, StorageBackend, TableDefinition};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("nora_enospc");

#[derive(Clone, Debug)]
struct EnospcBackend {
    bytes: Arc<Mutex<Vec<u8>>>,
    fail_sync: Arc<AtomicBool>,
}

impl EnospcBackend {
    fn new() -> Self {
        Self {
            bytes: Arc::new(Mutex::new(Vec::new())),
            fail_sync: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fail_sync_with_enospc(&self, enabled: bool) {
        self.fail_sync.store(enabled, Ordering::SeqCst);
    }

    fn enospc() -> io::Error {
        io::Error::from_raw_os_error(28)
    }
}

impl StorageBackend for EnospcBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.bytes.lock().unwrap().len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let bytes = self.bytes.lock().unwrap();
        let offset = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
        let end = offset
            .checked_add(out.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read overflow"))?;
        let source = bytes
            .get(offset..end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "short read"))?;
        out.copy_from_slice(source);
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        let len = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "length overflow"))?;
        self.bytes.lock().unwrap().resize(len, 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        if self.fail_sync.load(Ordering::SeqCst) {
            Err(Self::enospc())
        } else {
            Ok(())
        }
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let end = offset
            .checked_add(u64::try_from(data.len()).unwrap())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write overflow"))?;
        let mut bytes = self.bytes.lock().unwrap();
        let offset = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset overflow"))?;
        let end = usize::try_from(end)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "end overflow"))?;
        let destination = bytes
            .get_mut(offset..end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "short write"))?;
        destination.copy_from_slice(data);
        Ok(())
    }
}

fn commit_immediate_2pc(mut transaction: redb::WriteTransaction) -> Result<(), redb::CommitError> {
    transaction.set_durability(Durability::Immediate).unwrap();
    transaction.set_two_phase_commit(true);
    transaction.commit()
}

#[test]
fn immediate_two_phase_commit_survives_enospc_without_losing_last_commit() {
    let backend = EnospcBackend::new();
    let database = Database::builder()
        .set_cache_size(0)
        .create_with_backend(backend.clone())
        .unwrap();

    let transaction = database.begin_write().unwrap();
    {
        let mut table = transaction.open_table(TABLE).unwrap();
        table
            .insert(1, b"durable-before-enospc".as_slice())
            .unwrap();
    }
    commit_immediate_2pc(transaction).unwrap();

    let transaction = database.begin_write().unwrap();
    {
        let mut table = transaction.open_table(TABLE).unwrap();
        let oversized = vec![0x5a; 8 * 1024 * 1024];
        table.insert(2, oversized.as_slice()).unwrap();
    }
    backend.fail_sync_with_enospc(true);
    let error = commit_immediate_2pc(transaction).unwrap_err();
    assert!(
        error.to_string().contains("No space left on device"),
        "commit must surface ENOSPC, got: {error}"
    );
    backend.fail_sync_with_enospc(false);
    drop(database);

    let recovered = Database::builder()
        .set_cache_size(0)
        .create_with_backend(backend.clone())
        .unwrap();
    {
        let read = recovered.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(
            table.get(1).unwrap().unwrap().value(),
            b"durable-before-enospc"
        );
        assert!(table.get(2).unwrap().is_none());
    }

    let transaction = recovered.begin_write().unwrap();
    {
        let mut table = transaction.open_table(TABLE).unwrap();
        table
            .insert(3, b"durable-after-recovery".as_slice())
            .unwrap();
    }
    commit_immediate_2pc(transaction).unwrap();
    drop(recovered);

    let reopened = Database::builder()
        .set_cache_size(0)
        .create_with_backend(backend)
        .unwrap();
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(
        table.get(1).unwrap().unwrap().value(),
        b"durable-before-enospc"
    );
    assert_eq!(
        table.get(3).unwrap().unwrap().value(),
        b"durable-after-recovery"
    );
}
