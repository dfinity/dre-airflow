/// Basic Python-formatted data structure parser.
/// Needed to retrieve the rollout plan and deserialize into a Rust structure.
/// Python objects are quoted strings -- this parser does not parse them.
use serde::Deserialize;
use serde::Serialize;
use serde::de;
use serde::de::{
    DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess, Visitor,
};
#[cfg(feature = "trace_python")]
use std::any::type_name;
use std::fmt::{self, Display};
use std::ops::{AddAssign, MulAssign, Neg};
use std::result;
use std::str::FromStr;

#[derive(Debug)]
pub(crate) enum ErrorCode {
    TrailingCharacters,
    Eof,
    ExpectedBoolean,
    ExpectedInteger,
    ExpectedFloat,
    ExpectedString,
    Syntax,
    Custom,
    ExpectedNull,
    ExpectedArray,
    ExpectedArrayComma,
    ExpectedArrayEnd,
    ExpectedMap,
    ExpectedMapColon,
    ExpectedMapComma,
    ExpectedMapEnd,
    ExpectedEnum,
    ExpectedDateTime,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{:?}]", self)
    }
}

#[derive(Debug)]
pub(crate) struct ErrorImpl {
    pub(crate) code: ErrorCode,
    pub(crate) context: String,
    pub(crate) message: String,
    pub(crate) cause: Option<Box<dyn std::error::Error>>,
}

impl std::error::Error for ErrorImpl {}

impl ErrorImpl {
    fn with_context(code: ErrorCode, context: String) -> Self {
        Self {
            code,
            context,
            message: "".to_string(),
            cause: None,
        }
    }

    fn with_context_str(code: ErrorCode, context: &str) -> Self {
        let ctx = context.chars().take(40).collect::<String>();
        Self::with_context(code, ctx)
    }

    fn with_context_and_cause(
        code: ErrorCode,
        context: String,
        cause: Option<Box<dyn std::error::Error>>,
    ) -> ErrorImpl {
        ErrorImpl {
            code,
            context,
            message: "".to_string(),
            cause,
        }
    }
}

impl serde::de::Error for ErrorImpl {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self {
            code: ErrorCode::Custom,
            message: msg.to_string(),
            cause: None,
            context: "".to_string(),
        }
    }
}

impl Display for ErrorImpl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} error parsing Python {}{}",
            self.code,
            match self.context.as_str() {
                "" => "".to_string(),
                _ => format!("around text {}", self.context),
            },
            match &self.cause {
                None => match self.message.as_str() {
                    "" => "".to_string(),
                    _ => self.message.to_string(),
                },
                Some(s) => format!(": {}", s),
            }
        )
    }
}

pub(crate) type Result<T> = result::Result<T, ErrorImpl>;

const DATETIME_FORMATS: [&str; 4] = [
    "datetime.datetime(%Y, %m, %d, %H, %M, %S)",
    "datetime.datetime(%Y, %m, %d, %H, %M)",
    "datetime.datetime(%Y, %m, %d, %H)",
    "datetime.datetime@version=2(timestamp=%s%.f,tz=None)",
];

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PythonDateTime(chrono::NaiveDateTime);

impl TryFrom<&str> for PythonDateTime {
    type Error = chrono::ParseError;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let mut err = None;
        for k in DATETIME_FORMATS.iter() {
            let m = chrono::NaiveDateTime::parse_from_str(value, k);
            match m {
                Ok(v) => return Ok(PythonDateTime(v)),
                Err(e) => err = Some(e),
            }
        }
        Err(err.unwrap())
    }
}

struct PythonDateVisitor;

impl<'de> Visitor<'de> for PythonDateVisitor {
    type Value = PythonDateTime;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a datetime.datetime(...)")
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match Self::Value::try_from(v) {
            Ok(vv) => Ok(vv),
            Err(e) => Err(E::custom(format!(
                "could not parse {} into a valid date/time: {}",
                v, e
            ))),
        }
    }
}

impl<'de> serde::de::Deserialize<'de> for PythonDateTime {
    fn deserialize<D>(
        deserializer: D,
    ) -> std::result::Result<PythonDateTime, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_identifier(PythonDateVisitor)
    }
}

impl From<PythonDateTime> for chrono::DateTime<chrono::Utc> {
    fn from(val: PythonDateTime) -> Self {
        val.0.and_utc()
    }
}

pub(crate) struct PythonDeserializer<'de> {
    // This string starts with the input data and characters are truncated off
    // the beginning as data is parsed.
    input: &'de str,
}

impl<'de> PythonDeserializer<'de> {
    // By convention, `Deserializer` constructors are named like `from_xyz`.
    // That way basic use cases are satisfied by something like
    // `serde_json::from_str(...)` while advanced use cases that require a
    // deserializer can make one with `serde_json::Deserializer::from_str(...)`.
    pub(crate) fn from_str(input: &'de str) -> PythonDeserializer<'de> {
        PythonDeserializer { input }
    }
}

// By convention, the public API of a Serde deserializer is one or more
// `from_xyz` methods such as `from_str`, `from_bytes`, or `from_reader`
// depending on what Rust types the deserializer is able to consume as input.
//
// This basic deserializer supports only `from_str`.
pub(crate) fn from_str<'a, T>(s: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = PythonDeserializer::from_str(s);
    let t = T::deserialize(&mut deserializer)?;
    let _ = deserializer.chomp_whitespace();
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(ErrorImpl::with_context_str(
            ErrorCode::TrailingCharacters,
            deserializer.input,
        ))
    }
}

// SERDE IS NOT A PARSING LIBRARY. This impl block defines a few basic parsing
// functions from scratch. More complicated formats may wish to use a dedicated
// parsing library to help implement their Serde deserializer.
impl<'de> PythonDeserializer<'de> {
    // Look at the first character in the input without consuming it.
    fn peek_char(&mut self) -> Result<char> {
        self.input
            .chars()
            .next()
            .ok_or(ErrorImpl::with_context_str(ErrorCode::Eof, ""))
    }

    fn chomp_whitespace(&mut self) -> Result<char> {
        while self.peek_char()? == ' ' || self.peek_char()? == '\n' {
            let _ = self.next_char()?;
        }
        self.peek_char()
    }

    // Consume the first character in the input.
    fn next_char(&mut self) -> Result<char> {
        let ch = self.peek_char()?;
        self.input = &self.input[ch.len_utf8()..];
        Ok(ch)
    }

    // Parse the JSON identifier `true` or `false`.
    fn parse_bool(&mut self) -> Result<bool> {
        if self.input.starts_with("true") {
            self.input = &self.input["true".len()..];
            Ok(true)
        } else if self.input.starts_with("false") {
            self.input = &self.input["false".len()..];
            Ok(false)
        } else {
            Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedBoolean,
                self.input,
            ))
        }
    }

    // Parse a group of decimal digits as an unsigned integer of type T.
    //
    // This implementation is a bit too lenient, for example `001` is not
    // allowed in JSON. Also the various arithmetic operations can overflow and
    // panic or return bogus data. But it is good enough for example code!
    fn parse_unsigned<T>(&mut self) -> Result<T>
    where
        T: AddAssign<T> + MulAssign<T> + From<u8>,
    {
        self.chomp_whitespace()?;

        let mut int = match self.next_char()? {
            ch @ '0'..='9' => T::from(ch as u8 - b'0'),
            _ => {
                return Err(ErrorImpl::with_context_str(
                    ErrorCode::ExpectedInteger,
                    self.input,
                ));
            }
        };
        loop {
            match self.input.chars().next() {
                Some(ch @ '0'..='9') => {
                    self.input = &self.input[1..];
                    int *= T::from(10);
                    int += T::from(ch as u8 - b'0');
                }
                _ => {
                    return Ok(int);
                }
            }
        }
    }

    // Parse a possible minus sign followed by a group of decimal digits as a
    // signed integer of type T.
    fn parse_signed<T>(&mut self) -> Result<T>
    where
        T: Neg<Output = T> + AddAssign<T> + MulAssign<T> + From<i8>,
    {
        self.chomp_whitespace()?;

        // Optional minus sign, delegate to `parse_unsigned`, negate if negative.
        let neg = match self.peek_char()? {
            '-' => {
                let _ = self.next_char()?;
                true
            }
            _ => false,
        };

        self.chomp_whitespace()?;

        let mut int = match self.next_char()? {
            ch @ '0'..='9' => T::from((ch as u8 - b'0').try_into().unwrap()),
            _ => {
                return Err(ErrorImpl::with_context_str(
                    ErrorCode::ExpectedInteger,
                    self.input,
                ));
            }
        };
        loop {
            match self.input.chars().next() {
                Some(ch @ '0'..='9') => {
                    self.input = &self.input[1..];
                    int *= T::from(10);
                    int += T::from((ch as u8 - b'0').try_into().unwrap());
                }
                _ => {
                    if neg {
                        int = int.neg();
                    }
                    return Ok(int);
                }
            }
        }
    }

    fn parse_float(&mut self) -> Result<f64> {
        self.chomp_whitespace()?;

        let mut chars: String = "".to_string();

        if self.peek_char()? == '-' {
            let _ = self.next_char()?;
            chars += "-";
        };

        self.chomp_whitespace()?;

        let mut period_already = false;

        loop {
            match self.input.chars().next() {
                Some(ch @ '0'..='9') => {
                    self.input = &self.input[1..];
                    chars = chars + &ch.to_string();
                }
                Some('.') => match period_already {
                    false => {
                        self.input = &self.input[1..];
                        period_already = true;
                        chars += ".";
                    }
                    true => {
                        return Err(ErrorImpl::with_context_str(
                            ErrorCode::ExpectedFloat,
                            self.input,
                        ));
                    }
                },
                _ => {
                    break;
                }
            }
        }

        let converted = f64::from_str(&chars);

        match converted {
            Ok(f) => Ok(f),
            Err(e) => Err(ErrorImpl::with_context_and_cause(
                ErrorCode::ExpectedFloat,
                chars,
                Some(Box::new(e)),
            )),
        }
    }

    // Parse a string until the next '\'' character.
    //
    // Makes no attempt to handle escape sequences. What did you expect? This is
    // example code!
    fn parse_string(&mut self) -> Result<&'de str> {
        self.chomp_whitespace()?;

        let ch = self.next_char()?;
        if ch != '\'' {
            return Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedString,
                self.input,
            ));
        }
        match self.input.find('\'') {
            Some(len) => {
                let s = &self.input[..len];
                self.input = &self.input[len + 1..];
                Ok(s)
            }
            None => Err(ErrorImpl::with_context_str(ErrorCode::Eof, "")),
        }
    }

    fn parse_datetime(&mut self) -> Result<&'de str> {
        self.chomp_whitespace()?;
        if !self.input.starts_with("datetime.datetime(") {
            return Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedDateTime,
                self.input,
            ));
        }
        match self.input.find(')') {
            Some(len) => {
                let s = &self.input[..len + 1];
                self.input = &self.input[len + 1..];
                Ok(s)
            }
            None => Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedDateTime,
                self.input,
            )),
        }
    }
}

impl<'de> de::Deserializer<'de> for &mut PythonDeserializer<'de> {
    type Error = ErrorImpl;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing any of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.chomp_whitespace()?;

        match self.peek_char()? {
            'n' | 'N' => self.deserialize_unit(visitor),
            't' | 'f' => self.deserialize_bool(visitor),
            '\'' => self.deserialize_str(visitor),
            '0'..='9' | '-' => {
                let mut is_float = false;
                let mut is_negative = false;
                for c in self.input.chars() {
                    match c {
                        '0'..='9' => {}
                        '-' => is_negative = true,
                        '.' => is_float = true,
                        _ => {
                            break;
                        }
                    }
                }
                if is_float {
                    self.deserialize_f64(visitor)
                } else if is_negative {
                    self.deserialize_i64(visitor)
                } else {
                    self.deserialize_u64(visitor)
                }
            }
            '[' => self.deserialize_seq(visitor),
            '{' => self.deserialize_map(visitor),
            _ => Err(ErrorImpl::with_context_str(ErrorCode::Syntax, self.input)),
        }
    }

    // Uses the `parse_bool` parsing function defined above to read the JSON
    // identifier `true` or `false` from the input.
    //
    // Parsing refers to looking at the input and deciding that it contains the
    // JSON value `true` or `false`.
    //
    // Deserialization refers to mapping that JSON value into Serde's data
    // model by invoking one of the `Visitor` methods. In the case of JSON and
    // bool that mapping is straightforward so the distinction may seem silly,
    // but in other cases Deserializers sometimes perform non-obvious mappings.
    // For example the TOML format has a Datetime type and Serde's data model
    // does not. In the `toml` crate, a Datetime in the input is deserialized by
    // mapping it to a Serde data model "struct" type with a special name and a
    // single field containing the Datetime represented as a string.
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(self.parse_bool()?)
    }

    // The `parse_signed` function is generic over the integer type `T` so here
    // it is invoked with `T=i8`. The next 8 methods are similar.
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.parse_signed()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.parse_signed()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.parse_signed()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.parse_signed()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.parse_unsigned()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.parse_unsigned()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.parse_unsigned()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.parse_unsigned()?)
    }

    // Float parsing is stupidly hard.
    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let f = self.parse_float()?;
        let f2 = f as f32;

        visitor.visit_f32(f2)
    }

    // Float parsing is stupidly hard.
    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(self.parse_float()?)
    }

    // The `Serializer` implementation on the previous page serialized chars as
    // single-character strings so handle that representation here.
    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Parse a string, check that it is one character, call `visit_char`.
        visitor.visit_char(self.next_char()?)
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing str of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );

        visitor.visit_borrowed_str(self.parse_string()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    // The `Serializer` implementation on the previous page serialized byte
    // arrays as JSON arrays of bytes. Handle that representation here.
    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // An absent optional is represented as the JSON `null` and a present
    // optional is represented as just the contained value.
    //
    // As commented in `Serializer` implementation, this is a lossy
    // representation. For example the values `Some(())` and `None` both
    // serialize as just `null`. Unfortunately this is typically what people
    // expect when working with JSON. Other formats are encouraged to behave
    // more intelligently if possible.
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing option of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.chomp_whitespace()?;
        if self.input.starts_with("null") || self.input.starts_with("None") {
            self.input = &self.input["null".len()..];
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing unit of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.chomp_whitespace()?;
        if self.input.starts_with("null") || self.input.starts_with("None") {
            self.input = &self.input["null".len()..];
            visitor.visit_unit()
        } else {
            Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedNull,
                self.input,
            ))
        }
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing unit struct of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.chomp_whitespace()?;
        self.deserialize_unit(visitor)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain. That means not
    // parsing anything other than the contained value.
    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing newtype struct of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.chomp_whitespace()?;
        visitor.visit_newtype_struct(self)
    }

    // Deserialization of compound types like sequences and maps happens by
    // passing the visitor an "Access" object that gives it the ability to
    // iterate through the data contained in the sequence.
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing seq of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.chomp_whitespace()?;
        let ch = self.next_char()?;

        // Parse the opening bracket of the sequence.
        if ch == '[' {
            #[cfg(feature = "trace_python")]
            eprintln!("open square bracket");
            // Give the visitor access to each element of the sequence.
            let value = visitor.visit_seq(CommaSeparated::new(self))?;

            self.chomp_whitespace()?;

            // Parse the closing bracket of the sequence.
            if self.next_char()? == ']' {
                #[cfg(feature = "trace_python")]
                eprintln!("close square bracket");
                Ok(value)
            } else {
                Err(ErrorImpl::with_context_str(
                    ErrorCode::ExpectedArrayEnd,
                    self.input,
                ))
            }
        } else {
            Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedArray,
                self.input,
            ))
        }
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently.
    //
    // As indicated by the length parameter, the `Deserialize` implementation
    // for a tuple in the Serde data model is required to know the length of the
    // tuple before even looking at the input data.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing tuple of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.deserialize_seq(visitor)
    }

    // Tuple structs look just like sequences in JSON.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing tuple struct of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );
        self.deserialize_seq(visitor)
    }

    // Much like `deserialize_seq` but calls the visitors `visit_map` method
    // with a `MapAccess` implementation, rather than the visitor's `visit_seq`
    // method with a `SeqAccess` implementation.
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing map of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>(),
        );

        self.chomp_whitespace()?;
        let ch = self.next_char()?;

        // Parse the opening brace of the map.
        if ch == '{' {
            // Give the visitor access to each entry of the map.
            #[cfg(feature = "trace_python")]
            eprintln!(
                "map start at {}\n{:?}",
                type_name::<V::Value>(),
                self.input.chars().take(180).collect::<String>(),
            );
            let value = visitor.visit_map(CommaSeparated::new(self))?;

            self.chomp_whitespace()?;

            // Parse the closing brace of the map.
            if self.next_char()? == '}' {
                #[cfg(feature = "trace_python")]
                eprintln!(
                    "map end at {}\n{:?}",
                    type_name::<V::Value>(),
                    self.input.chars().take(180).collect::<String>(),
                );
                Ok(value)
            } else {
                Err(ErrorImpl::with_context_str(
                    ErrorCode::ExpectedMapEnd,
                    self.input,
                ))
            }
        } else {
            Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedMap,
                self.input,
            ))
        }
    }

    // Structs look just like maps in JSON.
    //
    // Notice the `fields` parameter - a "struct" in the Serde data model means
    // that the `Deserialize` implementation is required to know what the fields
    // are before even looking at the input data. Any key-value pairing in which
    // the fields cannot be known ahead of time is probably a map.
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing struct of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>()
        );
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing enum of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>()
        );

        self.chomp_whitespace()?;

        if self.peek_char()? == '\'' {
            // Visit a unit variant.
            visitor.visit_enum(self.parse_string()?.into_deserializer())
        } else if self.next_char()? == '{' {
            // Visit a newtype variant, tuple variant, or struct variant.
            let value = visitor.visit_enum(Enum::new(self))?;
            // Parse the matching close brace.
            if self.next_char()? == '}' {
                Ok(value)
            } else {
                Err(ErrorImpl::with_context_str(
                    ErrorCode::ExpectedMapEnd,
                    self.input,
                ))
            }
        } else {
            Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedEnum,
                self.input,
            ))
        }
    }

    // An identifier in Serde is the type that identifies a field of a struct or
    // the variant of an enum. In JSON, struct fields and enum variants are
    // represented as strings. In other formats they may be represented as
    // numeric indices.
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "deserializing identifier of {}\n{:?}",
            type_name::<V::Value>(),
            self.input.chars().take(180).collect::<String>()
        );
        self.chomp_whitespace()?;
        if self.input.starts_with("datetime.datetime(") {
            let datetime = self.parse_datetime()?;
            visitor.visit_str(datetime)
        } else {
            self.deserialize_str(visitor)
        }
    }

    // Like `deserialize_any` but indicates to the `Deserializer` that it makes
    // no difference which `Visitor` method is called because the data is
    // ignored.
    //
    // Some deserializers are able to implement this more efficiently than
    // `deserialize_any`, for example by rapidly skipping over matched
    // delimiters without paying close attention to the data in between.
    //
    // Some formats are not able to implement this at all. Formats that can
    // implement `deserialize_any` and `deserialize_ignored_any` are known as
    // self-describing.
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

// In order to handle commas correctly when deserializing a JSON array or map,
// we need to track whether we are on the first element or past the first
// element.
struct CommaSeparated<'a, 'de: 'a> {
    de: &'a mut PythonDeserializer<'de>,
    first: bool,
}

impl<'a, 'de> CommaSeparated<'a, 'de> {
    fn new(de: &'a mut PythonDeserializer<'de>) -> Self {
        CommaSeparated { de, first: true }
    }
}

// `SeqAccess` is provided to the `Visitor` to give it the ability to iterate
// through elements of the sequence.
impl<'de, 'a> SeqAccess<'de> for CommaSeparated<'a, 'de> {
    type Error = ErrorImpl;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "next element seed of {}\n{:?}",
            type_name::<T::Value>(),
            self.de.input.chars().take(180).collect::<String>(),
        );
        // Check if there are no more elements.
        if self.de.peek_char()? == ']' {
            return Ok(None);
        }

        self.de.chomp_whitespace()?;

        // Comma is required before every element except the first.
        if !self.first {
            let ch = self.de.next_char()?;
            if ch != ',' {
                return Err(ErrorImpl::with_context_str(
                    ErrorCode::ExpectedArrayComma,
                    self.de.input,
                ));
            }
        }
        self.first = false;

        // Deserialize an array element.
        seed.deserialize(&mut *self.de).map(Some)
    }
}

// `MapAccess` is provided to the `Visitor` to give it the ability to iterate
// through entries of the map.
impl<'de, 'a> MapAccess<'de> for CommaSeparated<'a, 'de> {
    type Error = ErrorImpl;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "next key seed of {}\n{:?}",
            type_name::<K::Value>(),
            self.de.input.chars().take(180).collect::<String>()
        );
        // Check if there are no more entries.
        if self.de.peek_char()? == '}' {
            return Ok(None);
        }

        self.de.chomp_whitespace()?;

        // Comma is required before every entry except the first.
        if !self.first {
            let ch = self.de.next_char()?;
            if ch != ',' {
                return Err(ErrorImpl::with_context_str(
                    ErrorCode::ExpectedMapComma,
                    self.de.input,
                ));
            }
        }
        self.first = false;
        // Deserialize a map key.
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        #[cfg(feature = "trace_python")]
        eprintln!(
            "next value seed of {}\n{:?}",
            type_name::<V::Value>(),
            self.de.input.chars().take(180).collect::<String>()
        );
        // It doesn't make a difference whether the colon is parsed at the end
        // of `next_key_seed` or at the beginning of `next_value_seed`. In this
        // case the code is a bit simpler having it here.
        self.de.chomp_whitespace()?;

        if self.de.next_char()? != ':' {
            return Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedMapColon,
                self.de.input,
            ));
        }
        // Deserialize a map value.
        seed.deserialize(&mut *self.de)
    }
}

struct Enum<'a, 'de: 'a> {
    de: &'a mut PythonDeserializer<'de>,
}

impl<'a, 'de> Enum<'a, 'de> {
    fn new(de: &'a mut PythonDeserializer<'de>) -> Self {
        Enum { de }
    }
}

// `EnumAccess` is provided to the `Visitor` to give it the ability to determine
// which variant of the enum is supposed to be deserialized.
//
// Note that all enum deserialization methods in Serde refer exclusively to the
// "externally tagged" enum representation.
impl<'de, 'a> EnumAccess<'de> for Enum<'a, 'de> {
    type Error = ErrorImpl;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        // The `deserialize_enum` method parsed a `{` character so we are
        // currently inside of a map. The seed will be deserializing itself from
        // the key of the map.
        let val = seed.deserialize(&mut *self.de)?;
        // Parse the colon separating map key from value.
        if self.de.next_char()? == ':' {
            Ok((val, self))
        } else {
            Err(ErrorImpl::with_context_str(
                ErrorCode::ExpectedMapColon,
                self.de.input,
            ))
        }
    }
}

// `VariantAccess` is provided to the `Visitor` to give it the ability to see
// the content of the single variant that it decided to deserialize.
impl<'de, 'a> VariantAccess<'de> for Enum<'a, 'de> {
    type Error = ErrorImpl;

    // If the `Visitor` expected this variant to be a unit variant, the input
    // should have been the plain string case handled in `deserialize_enum`.
    fn unit_variant(self) -> Result<()> {
        Err(ErrorImpl::with_context_str(
            ErrorCode::ExpectedString,
            self.de.input,
        ))
    }

    // Newtype variants are represented in JSON as `{ NAME: VALUE }` so
    // deserialize the value here.
    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.de)
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }` so
    // deserialize the sequence of data here.
    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self.de, visitor)
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }` so
    // deserialize the inner map here.
    fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_map(self.de, visitor)
    }
}
