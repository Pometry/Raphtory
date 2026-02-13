use either::Either;
use raphtory_api::core::{
    entities::properties::{
        meta::{LockedPropMapper, Meta, PropMapper},
        prop::{Prop, unify_types},
    },
    storage::dict_mapper::MaybeNew,
};

use crate::error::StorageError;

// TODO: Rename constant props to metadata
#[derive(Debug, Clone, Copy)]
pub enum PropType {
    Temporal,
    Constant,
}

pub enum PropsMetaWriter<'a, PN: AsRef<str>> {
    Change {
        props: Vec<PropEntry<'a, PN>>,
        mapper: LockedPropMapper<'a>,
        meta: &'a Meta,
    },
    NoChange {
        props: Vec<(PN, usize, Prop)>,
    },
}

pub enum PropEntry<'a, PN: AsRef<str> + 'a> {
    Change {
        name: PN,
        prop_id: Option<usize>,
        prop: Prop,
        _phantom: &'a (),
    },
    NoChange(PN, usize, Prop),
}

impl<'a, PN: AsRef<str>> PropsMetaWriter<'a, PN> {
    pub fn temporal(
        meta: &'a Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Self, StorageError> {
        Self::new(meta, meta.temporal_prop_mapper(), props)
    }

    pub fn constant(
        meta: &'a Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Self, StorageError> {
        Self::new(meta, meta.metadata_mapper(), props)
    }

    pub fn new(
        meta: &'a Meta,
        prop_mapper: &'a PropMapper,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Self, StorageError> {
        let locked_meta = prop_mapper.locked();

        let mut in_props = props
            .size_hint()
            .1
            .map(Vec::with_capacity)
            .unwrap_or_default();

        let mut no_type_changes = true;

        // See if any type unification is required while merging props
        for (prop_name, prop) in props {
            let dtype = prop.dtype();
            let outcome @ (_, _, type_check) = locked_meta
                .fast_proptype_check(prop_name.as_ref(), dtype)
                .map(|outcome| (prop_name, prop, outcome))?;
            let nothing_to_do = type_check.map(|x| x.is_right()).unwrap_or_default();

            no_type_changes &= nothing_to_do;
            in_props.push(outcome);
        }

        // If no type changes are required, we can just return the existing prop ids
        if no_type_changes {
            let props = in_props
                .into_iter()
                .filter_map(|(prop_name, prop, _)| {
                    locked_meta
                        .get_id(prop_name.as_ref())
                        .map(|id| (prop_name, id, prop))
                })
                .collect();

            return Ok(Self::NoChange { props });
        }

        let mut props = vec![];

        for (prop_name, prop, outcome) in in_props {
            props.push(Self::as_prop_entry(prop_name, prop, outcome));
        }

        Ok(Self::Change {
            props,
            mapper: locked_meta,
            meta,
        })
    }

    fn as_prop_entry(
        prop_name: PN,
        prop: Prop,
        outcome: Option<Either<usize, usize>>,
    ) -> PropEntry<'a, PN> {
        match outcome {
            Some(Either::Right(prop_id)) => PropEntry::NoChange(prop_name, prop_id, prop),
            Some(Either::Left(prop_id)) => PropEntry::Change {
                name: prop_name,
                prop_id: Some(prop_id),
                prop,
                _phantom: &(),
            },
            None => {
                // prop id doesn't exist so we grab the entry
                PropEntry::Change {
                    name: prop_name,
                    prop_id: None,
                    prop,
                    _phantom: &(),
                }
            }
        }
    }

    pub fn into_props_temporal(self) -> Result<Vec<(usize, Prop)>, StorageError> {
        self.into_props_inner(PropType::Temporal)
    }

    /// Returns temporal prop names, prop ids and prop values, along with their MaybeNew status.
    pub fn into_props_temporal_with_status(
        self,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, StorageError> {
        self.into_props_inner_with_status(PropType::Temporal)
    }

    pub fn into_props_const(self) -> Result<Vec<(usize, Prop)>, StorageError> {
        self.into_props_inner(PropType::Constant)
    }

    /// Returns constant prop names, prop ids and prop values, along with their MaybeNew status.
    pub fn into_props_const_with_status(
        self,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, StorageError> {
        self.into_props_inner_with_status(PropType::Constant)
    }

    pub fn into_props_inner(self, prop_type: PropType) -> Result<Vec<(usize, Prop)>, StorageError> {
        self.into_props_inner_with_status(prop_type).map(|props| {
            props
                .into_iter()
                .map(|maybe_new| {
                    let (_, prop_id, prop) = maybe_new.inner();
                    (prop_id, prop)
                })
                .collect()
        })
    }

    pub fn into_props_inner_with_status(
        self,
        prop_type: PropType,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, StorageError> {
        match self {
            Self::NoChange { props } => Ok(props
                .into_iter()
                .map(|(prop_name, prop_id, prop)| MaybeNew::Existing((prop_name, prop_id, prop)))
                .collect()),
            Self::Change {
                props,
                mapper,
                meta,
            } => {
                let mut prop_with_ids = vec![];

                drop(mapper);

                let mut mapper = match prop_type {
                    PropType::Temporal => meta.temporal_prop_mapper().write_locked(),
                    PropType::Constant => meta.metadata_mapper().write_locked(),
                };

                // Revalidate prop types
                let props = props
                    .into_iter()
                    .map(|entry| match entry {
                        PropEntry::NoChange(name, _, prop) => {
                            let new_entry = mapper
                                .fast_proptype_check(name.as_ref(), prop.dtype())
                                .map(|outcome| Self::as_prop_entry(name, prop, outcome))?;

                            Ok(new_entry)
                        }
                        PropEntry::Change { name, prop, .. } => {
                            let new_entry = mapper
                                .fast_proptype_check(name.as_ref(), prop.dtype())
                                .map(|outcome| Self::as_prop_entry(name, prop, outcome))?;

                            Ok(new_entry)
                        }
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?;

                for entry in props {
                    match entry {
                        PropEntry::NoChange(name, prop_id, prop) => {
                            prop_with_ids.push(MaybeNew::Existing((name, prop_id, prop)));
                        }
                        PropEntry::Change {
                            name,
                            prop_id: Some(prop_id),
                            prop,
                            ..
                        } => {
                            // prop_id already exists, so we need to unify the types
                            let new_prop_type = prop.dtype();
                            let existing_type = mapper.get_dtype(prop_id).unwrap();
                            let new_prop_type =
                                unify_types(&new_prop_type, existing_type, &mut false)?;

                            mapper.set_id_and_dtype(name.as_ref(), prop_id, new_prop_type);
                            prop_with_ids.push(MaybeNew::Existing((name, prop_id, prop)));
                        }
                        PropEntry::Change { name, prop, .. } => {
                            // prop_id doesn't exist, so we need to create a new one
                            let new_prop_type = prop.dtype();
                            let prop_id = mapper.new_id_and_dtype(name.as_ref(), new_prop_type);

                            prop_with_ids.push(MaybeNew::New((name, prop_id, prop)));
                        }
                    }
                }

                Ok(prop_with_ids)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use raphtory_api::core::storage::arc_str::ArcStr;

    #[test]
    fn test_props_meta_writer() {
        let meta = Meta::default();
        let props = vec![
            (ArcStr::from("prop1"), Prop::U32(0)),
            (ArcStr::from("prop2"), Prop::U32(1)),
        ];
        let writer = PropsMetaWriter::temporal(&meta, props.into_iter()).unwrap();
        let props = writer.into_props_temporal().unwrap();
        assert_eq!(props.len(), 2);

        assert_eq!(props, vec![(0, Prop::U32(0)), (1, Prop::U32(1))]);

        assert_eq!(meta.temporal_prop_mapper().keys().len(), 2);
    }

    #[test]
    fn complex_props_meta_writer() {
        let meta = Meta::default();
        let prop_list_map = Prop::list([Prop::map([("a", 1)]), Prop::map([("b", 2f64)])]);
        let props = vec![("a", prop_list_map.clone())];

        let writer = PropsMetaWriter::temporal(&meta, props.into_iter()).unwrap();
        let props = writer.into_props_temporal().unwrap();
        assert_eq!(props.len(), 1);

        assert_eq!(props, vec![(0, prop_list_map.clone())]);

        let expected_d_type = prop_list_map.dtype();

        assert_eq!(
            meta.temporal_prop_mapper().d_types().first().unwrap(),
            &expected_d_type
        );
    }

    #[test]
    fn test_fail_typecheck() {
        let meta = Meta::default();
        let prop1 = Prop::U32(0);
        let prop2 = Prop::U64(1);

        let writer =
            PropsMetaWriter::temporal(&meta, vec![(ArcStr::from("prop1"), prop1)].into_iter())
                .unwrap();
        let props = writer.into_props_temporal().unwrap();
        assert_eq!(props.len(), 1);

        assert_eq!(meta.temporal_prop_mapper().keys().len(), 1);
        assert!(meta.temporal_prop_mapper().get_id("prop1").is_some());

        let writer =
            PropsMetaWriter::temporal(&meta, vec![(ArcStr::from("prop1"), prop2)].into_iter());

        assert!(writer.is_err());
        assert_eq!(meta.temporal_prop_mapper().keys().len(), 1);
        assert!(meta.temporal_prop_mapper().get_id("prop1").is_some());
    }
}
