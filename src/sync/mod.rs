pub mod sqlite;

use std::collections::HashMap;
use std::hash::Hash;
use super::{ExecutedMigrationInfo, MigrationInfo};
use super::types::compare_versions;

pub trait Connection {
    type Err;
    type Trans: Transaction<Err = Self::Err>;

    fn ensure_migration_table(&self) -> Result<(), Self::Err>;
    fn load_existing_migrations(&self) -> Result<Vec<ExecutedMigrationInfo>, Self::Err>;
    fn in_transaction<F>(&self, f: F) -> Result<(), (bool, Self::Err)>
        where F: FnOnce(&Self::Trans) -> Result<(), Self::Err>;
}

pub trait Transaction {
    type Err;

    fn save_migration(&self, info: &ExecutedMigrationInfo) -> Result<(), Self::Err>;
}

pub struct Migration<T, E> {
    version: String,
    name: String,
    f: Box<dyn FnMut(&T) -> Result<(), E>>
}

impl <T, E> Migration<T, E> {
    pub fn new<V, N, F>(version: V, name: N, f: F) -> Migration<T, E>
        where V: Into<String>,
              N: Into<String>,
              F: FnMut(&T) -> Result<(), E> + 'static {
        Migration {
            version: version.into(),
            name: name.into(),
            f: Box::new(f)
        }
    }
}

impl <T, E> Into<MigrationInfo> for Migration<T, E> {
    fn into(self) -> MigrationInfo {
        MigrationInfo {
            version: self.version,
            name: self.name
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum MigrationError<E> {
    UnexpectedMigrations(Vec<MigrationInfo>),
    DatabaseError(E)
}

impl <E> From<E> for MigrationError<E> {
    fn from(e: E) -> Self {
        MigrationError::DatabaseError(e)
    }
}

type MigrationState<T, E> = Vec<(Option<Migration<T, E>>, Option<ExecutedMigrationInfo>)>;

pub fn migrate<C, T, E>(conn: &C, migrations: Vec<Migration<T, E>>) -> Result<(), MigrationError<E>>
    where T: Transaction<Err = E>,
          C: Connection<Trans = T, Err = E> {
    conn.ensure_migration_table()?;
    let available: HashMap<String, Migration<T, E>> = migrations.into_iter()
        .map(|m| (m.version.clone(), m))
        .collect();
    let existing: HashMap<String, ExecutedMigrationInfo> = conn.load_existing_migrations()?.into_iter()
        .map(|m| (m.migration.version.clone(), m))
        .collect();

    let migration_state = merge(available, existing).into_iter()
        .map(|(_, v)| v)
        .collect();
    check_unexpected_migrations(&migration_state)?;

    Ok(())
}

fn merge<K: Eq + Hash, V1, V2>(m1: HashMap<K, V1>, m2: HashMap<K, V2>) -> HashMap<K, (Option<V1>, Option<V2>)> {
    let mut result: HashMap<K, (Option<V1>, Option<V2>)> = m1.into_iter()
        .map(|(k, v)| (k, (Some(v), None)))
        .collect();
    for (k, v) in m2.into_iter() {
        let entry = result.entry(k).or_insert((None, None));
        entry.1 = Some(v);
    }
    result
}

fn check_unexpected_migrations<T, E>(migration_state: &MigrationState<T, E>) -> Result<(), MigrationError<E>> {
    let mut unexpected_migrations: Vec<MigrationInfo> = migration_state.iter()
        .filter(|(a, _)| a.is_none())
        .map(|(_, e)| e.as_ref().unwrap().migration.clone())
        .collect();
    unexpected_migrations.sort_unstable_by(|m1, m2| compare_versions(&m1.version, &m2.version));
    if !unexpected_migrations.is_empty() {
        return Err(MigrationError::UnexpectedMigrations(unexpected_migrations));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::*;

    fn executed_migration<V, N>(sequence: u32, version: V, name: N) -> ExecutedMigrationInfo
        where V: Into<String>,
              N: Into<String> {
        ExecutedMigrationInfo {
            sequence,
            migration: migration(version, name)
        }
    }

    fn migration<V, N>(version: V, name: N) -> MigrationInfo
        where V: Into<String>,
              N: Into<String> {
        MigrationInfo {
            version: version.into().parse().unwrap(),
            name: name.into()
        }
    }

    #[test]
    fn non_existent_migrations() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();

        let migrations = vec!(
            Migration::new("1.0.0", "test_migration", |_| Ok(()))
        );
        connection.ensure_migration_table().unwrap();
        connection.in_transaction(|t| {
            t.save_migration(&executed_migration(1, "0.0.1", "fake1"))?;
            t.save_migration(&executed_migration(2, "0.0.2", "fake2"))?;
            t.save_migration(&executed_migration(3, "0.0.10", "fake3"))?;
            Ok(())
        }).unwrap();

        let actual = migrate(&connection, migrations);
        match actual {
            Err(MigrationError::UnexpectedMigrations(m)) => assert_eq!(vec!(
                    migration("0.0.1", "fake1"),
                    migration("0.0.2", "fake2"),
                    migration("0.0.10", "fake3")
                ),
                m),
            o => panic!("Unexpected result {:?}", o)
        }
    }
}