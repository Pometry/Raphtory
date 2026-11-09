use crate::error::StorageError;
use either::Either;
use raphtory_api::core::entities::properties::{
    meta::{Meta, PropMapper, StagedPropMapper},
    prop::{Prop, PropType},
};

pub enum PropsMetaWriter<'a, PN: AsRef<str>> {
    Change {
        resolved: Vec<(PN, usize, Prop)>,
        change: Vec<(PN, Option<(usize, PropType)>, Prop)>,
        mapper: StagedPropMapper<'a>,
    },
    NoChange {
        props: Vec<(PN, usize, Prop)>,
    },
}

impl<'a, PN: AsRef<str>> PropsMetaWriter<'a, PN> {
    pub fn temporal(
        meta: &'a Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Self, StorageError> {
        Self::new(meta.temporal_prop_mapper(), props)
    }

    pub fn constant(
        meta: &'a Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Self, StorageError> {
        Self::new(meta.metadata_mapper(), props)
    }

    pub fn new(
        prop_mapper: &'a PropMapper,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Self, StorageError> {
        let locked_meta = prop_mapper.locked();
        let mut resolved = vec![];
        let mut change = vec![];

        // See if any type unification is required while merging props
        for (prop_name, prop) in props {
            let dtype = prop.dtype();
            match locked_meta.fast_proptype_check(prop_name.as_ref(), dtype)? {
                None => {
                    change.push((prop_name, None, prop));
                }
                Some(Either::Left(id)) => {
                    change.push((prop_name, Some(id), prop));
                }
                Some(Either::Right(id)) => resolved.push((prop_name, id, prop)),
            }
        }

        // If no type changes are required, we can just return the existing prop ids
        if change.is_empty() {
            return Ok(Self::NoChange { props: resolved });
        }

        drop(locked_meta);
        let staged_meta = prop_mapper.stage();

        let mut change_and_unified = Vec::with_capacity(change.len());
        for (name, id, prop) in change {
            match id {
                Some(id) => {
                    match staged_meta.recheck_and_unify(name.as_ref(), id, prop.dtype())? {
                        Some(unified_type) => {
                            change_and_unified.push((name, Some((id, unified_type)), prop));
                        }
                        None => {
                            resolved.push((name, id, prop));
                        }
                    }
                }
                None => {
                    let outcome = staged_meta.resolve_and_unify(name.as_ref(), prop.dtype())?;
                    match outcome {
                        Some(Either::Right(id)) => {
                            resolved.push((name, id, prop));
                        }
                        Some(Either::Left(unified)) => {
                            change_and_unified.push((name, Some(unified), prop));
                        }
                        None => {
                            change_and_unified.push((name, None, prop));
                        }
                    }
                }
            }
        }

        if change_and_unified.is_empty() {
            Ok(Self::NoChange { props: resolved })
        } else {
            Ok(Self::Change {
                resolved,
                change: change_and_unified,
                mapper: staged_meta,
            })
        }
    }

    /// iterator over references to the already resolved properties, including those that need unifying, excluding unresolved properties
    pub fn resolved(&self) -> impl Iterator<Item = (usize, &Prop)> {
        match self {
            PropsMetaWriter::Change {
                resolved, change, ..
            } => Either::Left(
                resolved.iter().map(|(_, id, prop)| (*id, prop)).chain(
                    change
                        .iter()
                        .filter_map(|(_, id, prop)| id.as_ref().map(|(id, _)| (*id, prop))),
                ),
            ),
            PropsMetaWriter::NoChange { props } => {
                Either::Right(props.iter().map(|(_, id, prop)| (*id, prop)))
            }
        }
    }

    pub fn into_props(self) -> Vec<(usize, Prop)> {
        self.into_props_and_names().1
    }

    pub fn into_props_and_names(self) -> (Vec<PN>, Vec<(usize, Prop)>) {
        match self {
            Self::NoChange { props } => props
                .into_iter()
                .map(|(prop_name, prop_id, prop)| (prop_name, (prop_id, prop)))
                .unzip(),
            Self::Change {
                resolved,
                change,
                mapper,
            } => {
                let mut mapper = mapper.upgrade();
                resolved
                    .into_iter()
                    .map(|(prop_name, prop_id, prop)| (prop_name, (prop_id, prop)))
                    .chain(
                        change
                            .into_iter()
                            .map(|(prop_name, prop_id, prop)| match prop_id {
                                Some((id, unified_type)) => {
                                    mapper.set_dtype(id, unified_type);
                                    (prop_name, (id, prop))
                                }
                                None => {
                                    let id =
                                        mapper.new_id_and_dtype(prop_name.as_ref(), prop.dtype());
                                    (prop_name, (id, prop))
                                }
                            }),
                    )
                    .unzip()
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
        let props = writer.into_props();
        assert_eq!(props.len(), 2);

        assert_eq!(props, vec![(0, Prop::U32(0)), (1, Prop::U32(1))]);

        assert_eq!(meta.temporal_prop_mapper().keys().len(), 2);
    }

    #[test]
    fn complex_props_meta_writer() {
        let meta = Meta::default();
        let prop_list_map = Prop::list([Prop::map([("a", 1)]), Prop::map([("b", 2f64)])]).unwrap();
        let props = vec![("a", prop_list_map.clone())];

        let writer = PropsMetaWriter::temporal(&meta, props.into_iter()).unwrap();
        let props = writer.into_props();
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
        let props = writer.into_props();
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
