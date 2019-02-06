pub mod sqlite;

use std::collections::HashMap;
use std::hash::Hash;
use super::{ExecutedMigrationInfo, MigrationInfo, ParseVersionError, Version};

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

pub trait Migrations {
    type C: Connection;

    fn all_migrations(&self) -> Vec<MigrationInfo>;
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

type MigrationState = Vec<(Option<MigrationInfo>, Option<ExecutedMigrationInfo>)>;

pub fn migrate<C, T, E, M>(conn: &C, migrations: &M) -> Result<(), MigrationError<E>>
    where T: Transaction<Err = E>,
          C: Connection<Trans = T, Err = E>,
          M: Migrations<C = C> {
    conn.ensure_migration_table()?;
    let available: HashMap<Version, MigrationInfo> = migrations.all_migrations().into_iter()
        .map(|m| (m.version.clone(), m))
        .collect();
    let existing: HashMap<Version, ExecutedMigrationInfo> = conn.load_existing_migrations()?.into_iter()
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

fn check_unexpected_migrations<E>(migration_state: &MigrationState) -> Result<(), MigrationError<E>> {
    let mut unexpected_migrations: Vec<MigrationInfo> = migration_state.iter()
        .filter(|(a, _)| a.is_none())
        .map(|(_, e)| e.as_ref().unwrap().migration.clone())
        .collect();
    unexpected_migrations.sort_unstable_by(|m1, m2| m1.version.cmp(&m2.version));
    if !unexpected_migrations.is_empty() {
        return Err(MigrationError::UnexpectedMigrations(unexpected_migrations));
    }
    Ok(())
}

pub struct MigrationsBuilder<C, E> {
    migrations: HashMap<Version, (MigrationInfo, Box<dyn FnOnce(C) -> Result<(), E>>)>
}

impl <C, E> MigrationsBuilder<C, E> {
    pub fn new() -> MigrationsBuilder<C, E> {
        MigrationsBuilder {
            migrations: HashMap::new()
        }
    }

    pub fn add_migration<V, S, F>(&mut self, version: V, name: S, f: F) -> Result<(), ParseVersionError>
        where V: Into<String>,
              S: Into<String>,
              F: FnOnce(C) -> Result<(), E> + 'static {
        let version: Version = version.into().parse()?;
        let migration = MigrationInfo {
            version: version.clone(),
            name: name.into()
        };
        self.migrations.insert(version, (migration, Box::new(f)));
        Ok(())
    }
}

impl <C, E> Migrations for MigrationsBuilder<C, E>
    where C: Connection<Err = E> {
    type C = C;

    fn all_migrations(&self) -> Vec<MigrationInfo> {
        self.migrations.values()
            .map(|(m, _)| m.clone())
            .collect()
    }
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

        let mut migrations: MigrationsBuilder<rusqlite::Connection, rusqlite::Error> = MigrationsBuilder::new();
        migrations.add_migration("1.0.0", "test_migration", |_| Ok(())).unwrap();
        connection.ensure_migration_table().unwrap();
        connection.in_transaction(|t| {
            t.save_migration(&executed_migration(1, "0.0.1", "fake1"))?;
            t.save_migration(&executed_migration(2, "0.0.2", "fake2"))?;
            t.save_migration(&executed_migration(3, "0.0.3", "fake3"))?;
            Ok(())
        }).unwrap();

        let actual = migrate(&connection, &migrations);
        match actual {
            Err(MigrationError::UnexpectedMigrations(m)) => assert_eq!(vec!(
                    migration("0.0.1", "fake1"),
                    migration("0.0.2", "fake2"),
                    migration("0.0.3", "fake3")
                ),
                m),
            o => panic!("Unexpected result {:?}", o)
        }
    }
}