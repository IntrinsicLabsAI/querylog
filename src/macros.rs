#[macro_export]
macro_rules! row_vec {
    [$($a:expr),*] => {{
        let fields :Vec<$crate::FieldValue> = vec![
            $(
                $a.into(),
            )*
        ];

        fields
    }};
}

pub use row_vec;
