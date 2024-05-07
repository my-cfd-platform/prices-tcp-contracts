use std::{fmt::Debug, str::FromStr};

use rust_extensions::date_time::DateTimeAsMicroseconds;

pub fn to_fix_date_string(src: DateTimeAsMicroseconds) -> String {
    let dt = src.to_chrono_utc();
    dt.format("%Y%m%d-%H:%M:%S.%3f").to_string()
}

pub fn parse_fix_date(date: &str) -> DateTimeAsMicroseconds {
    let year = parse_number(date, &date[0..4]);
    let month = parse_number(date, &date[4..6]);
    let day = parse_number(date, &date[6..8]);
    let hour = parse_number(date, &date[9..11]);
    let min = parse_number(date, &date[12..14]);
    let sec = parse_number(date, &date[15..17]);
    let micros: i64 = parse_number(date, &date[18..21]);

    DateTimeAsMicroseconds::create(year, month, day, hour, min, sec, micros * 1000)
}

pub fn parse_tcp_feed_date(date: &[u8]) -> DateTimeAsMicroseconds {
    let year = parse_number_from_slice(date, &date[0..4]);
    let month = parse_number_from_slice(date, &date[4..6]);
    let day = parse_number_from_slice(date, &date[6..8]);
    let hour = parse_number_from_slice(date, &date[8..10]);
    let min = parse_number_from_slice(date, &date[10..12]);
    let sec = parse_number_from_slice(date, &date[12..14]);
    let micros: i64 = parse_number_from_slice(date, &date[15..18]);

    DateTimeAsMicroseconds::create(year, month, day, hour, min, sec, micros * 1000)
}

fn parse_number<TResult: FromStr + Debug>(date: &str, src: &str) -> TResult {
    match src.parse() {
        Ok(result) => result,
        Err(_) => {
            panic!("Unknown Date format: '{}'", date);
        }
    }
}

fn parse_number_from_slice<TResult: FromStr + Debug>(date: &[u8], src: &[u8]) -> TResult {
    unsafe {
        parse_number(
            std::str::from_utf8_unchecked(date),
            std::str::from_utf8_unchecked(src),
        )
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_parse_date() {
        let date = "20240425-17:28:02.629";
        let date: rust_extensions::date_time::DateTimeAsMicroseconds = super::parse_fix_date(date);
        assert_eq!(&date.to_rfc3339()[..23], "2024-04-25T17:28:02.629");
    }
}
