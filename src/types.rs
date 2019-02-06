use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationInfo {
    pub version: String,
    pub name: String
}

#[derive(Debug, Clone)]
pub struct ExecutedMigrationInfo {
    pub migration: MigrationInfo,
    pub sequence: u32
}

pub fn compare_versions<V1, V2>(v1: V1, v2: V2) -> Ordering
    where V1: AsRef<str>,
          V2: AsRef<str> {
    let mut v1_iter = v1.as_ref().split(".");
    let mut v2_iter = v2.as_ref().split(".");
    loop {
        match (v1_iter.next(), v2_iter.next()) {
            (None, None) => return Ordering::Equal,
            (None, _) => return Ordering::Less,
            (_, None) => return Ordering::Greater,
            (Some(v1), Some(v2)) => {
                let res = match (v1.parse::<u32>(), v2.parse::<u32>()) {
                    (Ok(i1), Ok(i2)) => i1.cmp(&i2),
                    _ => v1.cmp(v2)
                };
                if res != Ordering::Equal {
                    return res;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use super::compare_versions;

    #[test]
    fn equals() {
        assert_eq!(Ordering::Equal, compare_versions("1.0.0", "1.0.0"));
    }

    #[test]
    fn double_digit_less() {
        assert_eq!(Ordering::Less, compare_versions("2.0.0", "10.0.0"));
    }

    #[test]
    fn major_less() {
        assert_eq!(Ordering::Less, compare_versions("1.0.0", "2.0.0"));
    }

    #[test]
    fn minor_less() {
        assert_eq!(Ordering::Less, compare_versions("1.1.1", "1.2.0"));
    }
}