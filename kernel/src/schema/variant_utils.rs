//! Utility functions for the variant type and variant-related table features.

use crate::actions::Protocol;
use crate::schema::{DataType, Schema, SchemaTransform, StructField};
use crate::table_features::{ReaderFeature, WriterFeature};
use crate::utils::require;
use crate::{DeltaResult, Error};
use std::borrow::Cow;

/// Simple API used to obtain the unshredded Variant struct schema
pub fn unshredded_variant_schema() -> DataType {
    DataType::variant_type([
        StructField::nullable("metadata", DataType::BINARY),
        StructField::nullable("value", DataType::BINARY),
    ])
}

/// Simple API to test if a given DataType refers to an unshredded Variant.
pub fn is_unshredded_variant(s: &DataType) -> bool {
    s == &unshredded_variant_schema()
}

/// Schema visitor that checks if any column in the schema uses VARIANT type
#[derive(Debug, Default)]
pub(crate) struct UsesVariant(pub(crate) bool);

impl<'a> SchemaTransform<'a> for UsesVariant {
    fn transform_variant(&mut self, _: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.0 = true;
        None
    }
}

pub(crate) fn validate_variant_type_feature_support(
    schema: &Schema,
    protocol: &Protocol,
) -> DeltaResult<()> {
    // Both the reader and writer need to have either the VariantType or the VariantTypePreview
    // features.
    if (!protocol.has_reader_feature(&ReaderFeature::VariantType)
        && !protocol.has_reader_feature(&ReaderFeature::VariantTypePreview))
        || (!protocol.has_writer_feature(&WriterFeature::VariantType)
            && !protocol.has_writer_feature(&WriterFeature::VariantTypePreview))
    {
        let mut uses_variant = UsesVariant::default();
        let _ = uses_variant.transform_struct(schema);
        require!(
            !uses_variant.0,
            Error::unsupported(
                "Table contains VARIANT columns but does not have the required 'variantType' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::Protocol;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::{ReaderFeature, WriterFeature};

    #[test]
    fn test_is_unshredded_variant() {
        assert!(!is_unshredded_variant(
            &DataType::variant_type([
                StructField::nullable("value", DataType::BINARY),
                StructField::nullable("metadata", DataType::BINARY),
                StructField::nullable("another_field", DataType::BINARY),
            ])
            .into()
        ));
        assert!(is_unshredded_variant(
            &DataType::variant_type([
                StructField::nullable("value", DataType::BINARY),
                StructField::nullable("metadata", DataType::BINARY),
            ])
            .into()
        ));
    }

    #[test]
    fn test_variant_feature_validation() {
        let features = vec![
            (ReaderFeature::VariantType, WriterFeature::VariantType),
            (
                ReaderFeature::VariantTypePreview,
                WriterFeature::VariantTypePreview,
            ),
        ];
        let schema_with_variant = StructType::new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("v", unshredded_variant_schema(), true),
        ]);

        let schema_without_variant = StructType::new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);

        // Nested schema with VARIANT
        let nested_schema_with_variant = StructType::new([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new([StructField::new(
                    "inner_v",
                    unshredded_variant_schema(),
                    true,
                )]))),
                true,
            ),
        ]);
        features
            .iter()
            .for_each(|(variant_reader, variant_writer)| {
                // Protocol with variantType features
                let protocol_with_features =
                    Protocol::try_new(3, 7, Some([variant_reader]), Some([variant_writer]))
                        .unwrap();

                // Protocol without variantType features
                let protocol_without_features = Protocol::try_new(
                    3,
                    7,
                    Some::<Vec<String>>(vec![]),
                    Some::<Vec<String>>(vec![]),
                )
                .unwrap();

                // Protocol without variantType writer feature
                let protocol_without_writer_feature =
                    Protocol::try_new(3, 7, Some([variant_reader]), Some::<Vec<String>>(vec![]))
                        .unwrap();

                // Protocol without variantType reader feature
                let protocol_without_reader_feature =
                    Protocol::try_new(3, 7, Some::<Vec<String>>(vec![]), Some([variant_writer]))
                        .unwrap();

                // Schema with VARIANT + Protocol with features = OK
                validate_variant_type_feature_support(
                    &schema_with_variant,
                    &protocol_with_features,
                )
                .expect("Should succeed when features are present");

                // Schema without VARIANT + Protocol without features = OK
                validate_variant_type_feature_support(
                    &schema_without_variant,
                    &protocol_without_features,
                )
                .expect("Should succeed when no VARIANT columns are present");

                // Schema without VARIANT + Protocol with features = OK
                validate_variant_type_feature_support(
                    &schema_without_variant,
                    &protocol_with_features,
                )
                .expect("Should succeed when no VARIANT columns are present, even with features");

                // Schema with VARIANT + Protocol without features = ERROR
                let result = validate_variant_type_feature_support(
                    &schema_with_variant,
                    &protocol_without_features,
                );
                assert!(
                    result.is_err(),
                    "Should fail when VARIANT columns are present but features are missing"
                );
                assert!(result.unwrap_err().to_string().contains("variantType"));

                let result = validate_variant_type_feature_support(
                    &nested_schema_with_variant,
                    &protocol_without_features,
                );
                assert!(
                    result.is_err(),
                    "Should fail for nested VARIANT columns when features are missing"
                );

                // Schema with VARIANT + Protocol without writer feature = ERROR
                let result = validate_variant_type_feature_support(
                    &schema_with_variant,
                    &protocol_without_writer_feature,
                );
                assert!(
                    result.is_err(),
                    "Should fail when VARIANT columns are present but writer feature is missing"
                );
                assert!(result.unwrap_err().to_string().contains("variantType"));

                // Schema with VARIANT + Protocol without reader feature = ERROR
                let result = validate_variant_type_feature_support(
                    &schema_with_variant,
                    &protocol_without_reader_feature,
                );
                assert!(
                    result.is_err(),
                    "Should fail when VARIANT columns are present but reader feature is missing"
                );
                assert!(result.unwrap_err().to_string().contains("variantType"));
            });
    }
}
