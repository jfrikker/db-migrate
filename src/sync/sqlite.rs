use super::super::{ExecutedMigrationInfo, MigrationInfo};

impl super::Connection for rusqlite::Connection {
    type Err = rusqlite::Error;
    type Trans = Self;

    fn ensure_migration_table(&self) -> Result<(), Self::Err> {
        self.execute(r"
            CREATE TABLE IF NOT EXISTS migration (
                sequence integer not null primary key,
                version text not null unique,
                name text not null,
                applied_at text
            )
        ", rusqlite::NO_PARAMS)
        .map(|_| ())
    }

    fn load_existing_migrations(&self) -> Result<Vec<ExecutedMigrationInfo>, Self::Err> {
        self.prepare("SELECT sequence, version, name, applied_at FROM migration")?
            .query_map(rusqlite::NO_PARAMS, |row| {
                ExecutedMigrationInfo {
                    sequence: row.get(0),
                    migration: MigrationInfo {
                        version: row.get(1),
                        name: row.get(2)
                    }
                }
            })?
            .collect()
    }

    fn in_transaction<F>(&self, f: F) -> Result<(), (bool, Self::Err)>
        where F: FnOnce(&Self::Trans) -> Result<(), Self::Err> {
        f(self).map_err(|e| (false, e))
    }
}

impl super::Transaction for rusqlite::Connection {
    type Err = rusqlite::Error;

    fn save_migration(&self, info: &ExecutedMigrationInfo) -> Result<(), Self::Err> {
        let version_str = format!("{}", info.migration.version);
        let params: [&rusqlite::types::ToSql;3] = [
            &info.sequence,
            &version_str,
            &info.migration.name
        ];

        self.execute(r"
            insert into migration
            (sequence, version, name)
            values (?1, ?2, ?3)
        ", &params)
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::super::super::*;

    #[test]
    fn ensure_migration_table() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.ensure_migration_table().unwrap();
    }

    #[test]
    fn save_migration() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.ensure_migration_table().unwrap();

        let migration = ExecutedMigrationInfo {
            sequence: 1,
            migration: MigrationInfo {
                version: "1.0.0".parse().unwrap(),
                name: "test_migration".to_owned()
            }
        };
        connection.in_transaction(|t| t.save_migration(&migration)).unwrap();

        connection.in_transaction(|t| {
            assert_eq!(migration.migration.version, 
                t.load_existing_migrations().unwrap().get(0).unwrap().migration.version);
            Ok(())
        }).unwrap();
    }
}