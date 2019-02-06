pub mod sync;

use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct Version {
    parts: Vec<u32>
}

#[derive(Debug)]
pub enum ParseVersionError {
    ParseIntError(ParseIntError)
}

impl From<ParseIntError> for ParseVersionError {
    fn from(c: ParseIntError) -> Self {
        ParseVersionError::ParseIntError(c)
    }
}

impl FromStr for Version {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Result<Vec<u32>, ParseIntError> = s.split(".")
            .map(|p| p.parse())
            .collect();
        let parts = parts?;
        Ok(Version {
            parts
        })
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut iter = self.parts.iter();
        for p in iter.next() {
            write!(formatter, "{}", p)?;
        }
        for p in iter {
            write!(formatter, ".{}", p)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationInfo {
    pub version: Version,
    pub name: String
}

#[derive(Debug, Clone)]
pub struct ExecutedMigrationInfo {
    pub migration: MigrationInfo,
    pub sequence: u32
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use super::Version;

    #[test]
    fn equals() {
        assert!(Version::from_str("1.0.0").unwrap() == Version::from_str("1.0.0").unwrap());
    }

    #[test]
    fn major_less() {
        assert!(Version::from_str("1.1.0").unwrap() < Version::from_str("2.0.0").unwrap());
    }

    #[test]
    fn minor_less() {
        assert!(Version::from_str("1.1.1").unwrap() < Version::from_str("1.2.0").unwrap());
    }

    #[test]
    fn display() {
        assert_eq!("1.2.3.4.5", format!("{}", Version::from_str("1.2.3.4.5").unwrap()));
    }
}