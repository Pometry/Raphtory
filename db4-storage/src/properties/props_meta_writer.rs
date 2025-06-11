use db4_common::error::DBV4Error;
use either::Either;
use raphtory_api::core::entities::properties::{
    meta::{LockedPropMapper, Meta, PropMapper},
    prop::{Prop, unify_types},
};

pub enum PropsMetaWriter<'a, PN: AsRef<str>> {
    Change {
        props: Vec<PropEntry<'a, PN>>,
        mapper: LockedPropMapper<'a>,
        meta: &'a Meta,
    },
    NoChange {
        props: Vec<(usize, Prop)>,
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
        props: impl ExactSizeIterator<Item = (PN, Prop)>,
    ) -> Result<Self, DBV4Error> {
        Self::new(meta, meta.temporal_prop_meta(), props)
    }

    pub fn constant(
        meta: &'a Meta,
        props: impl ExactSizeIterator<Item = (PN, Prop)>,
    ) -> Result<Self, DBV4Error> {
        Self::new(meta, meta.const_prop_meta(), props)
    }

    pub fn new(
        meta: &'a Meta,
        prop_mapper: &'a PropMapper,
        props: impl ExactSizeIterator<Item = (PN, Prop)>,
    ) -> Result<Self, DBV4Error> {
        let locked_meta = prop_mapper.locked();

        let mut in_props = Vec::with_capacity(props.len());

        let mut no_type_changes = true;
        for (prop_name, prop) in props {
            let dtype = prop.dtype();
            let outcome @ (_, _, type_check) = locked_meta
                .fast_proptype_check(prop_name.as_ref(), dtype)
                .map(|outcome| (prop_name, prop, outcome))?;
            let nothing_to_do = type_check.map(|x| x.is_right()).unwrap_or_default();
            no_type_changes &= nothing_to_do;
            in_props.push(outcome);
        }

        if no_type_changes {
            return Ok(Self::NoChange {
                props: in_props
                    .into_iter()
                    .filter_map(|(prop_name, prop, _)| {
                        Some((locked_meta.get_id(prop_name.as_ref())?, prop))
                    })
                    .collect(),
            });
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

    pub fn into_props_temporal(self) -> Result<Vec<(usize, Prop)>, DBV4Error> {
        self.into_props_inner(|mapper| mapper.temporal_prop_meta())
    }

    pub fn into_props_const(self) -> Result<Vec<(usize, Prop)>, DBV4Error> {
        self.into_props_inner(|mapper| mapper.const_prop_meta())
    }

    pub fn into_props_inner(
        self,
        mapper_fn: impl Fn(&Meta) -> &PropMapper,
    ) -> Result<Vec<(usize, Prop)>, DBV4Error> {
        match self {
            Self::NoChange { props } => Ok(props),
            Self::Change {
                props,
                mapper,
                meta,
            } => {
                let mut prop_with_ids = vec![];
                drop(mapper);
                let mut mapper = mapper_fn(meta).write_locked();

                // revalidate
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
                    .collect::<Result<Vec<_>, DBV4Error>>()?;

                for entry in props {
                    match entry {
                        PropEntry::NoChange(_, prop_id, prop) => {
                            prop_with_ids.push((prop_id, prop));
                        }
                        PropEntry::Change {
                            name,
                            prop_id: Some(prop_id),
                            prop,
                            ..
                        } => {
                            let new_prop_type = prop.dtype();
                            let existing_type = mapper.get_dtype(prop_id).unwrap();
                            let new_prop_type =
                                unify_types(&new_prop_type, existing_type, &mut false)?;
                            mapper.set_id_and_dtype(name.as_ref(), prop_id, new_prop_type);
                            prop_with_ids.push((prop_id, prop));
                        }
                        PropEntry::Change { name, prop, .. } => {
                            let new_prop_type = prop.dtype();
                            let prop_id = mapper.new_id_and_dtype(name.as_ref(), new_prop_type);
                            prop_with_ids.push((prop_id, prop));
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

    use raphtory_api::core::storage::arc_str::ArcStr;

    use super::*;

    #[test]
    fn test_props_meta_writer() {
        let meta = Meta::new();
        let props = vec![
            (ArcStr::from("prop1"), Prop::U32(0)),
            (ArcStr::from("prop2"), Prop::U32(1)),
        ];
        let writer = PropsMetaWriter::temporal(&meta, props.into_iter()).unwrap();
        let props = writer.into_props_temporal().unwrap();
        assert_eq!(props.len(), 2);

        assert_eq!(props, vec![(0, Prop::U32(0)), (1, Prop::U32(1))]);

        assert_eq!(meta.temporal_prop_meta().len(), 2);
    }

    #[test]
    fn test_fail_typecheck() {
        let meta = Meta::new();
        let prop1 = Prop::U32(0);
        let prop2 = Prop::U64(1);

        let writer =
            PropsMetaWriter::temporal(&meta, vec![(ArcStr::from("prop1"), prop1)].into_iter())
                .unwrap();
        let props = writer.into_props_temporal().unwrap();
        assert_eq!(props.len(), 1);

        assert!(meta.temporal_prop_meta().len() == 1);
        assert!(meta.temporal_prop_meta().get_id("prop1").is_some());

        let writer =
            PropsMetaWriter::temporal(&meta, vec![(ArcStr::from("prop1"), prop2)].into_iter());

        assert!(writer.is_err());
        assert!(meta.temporal_prop_meta().len() == 1);
        assert!(meta.temporal_prop_meta().get_id("prop1").is_some());
    }
}
