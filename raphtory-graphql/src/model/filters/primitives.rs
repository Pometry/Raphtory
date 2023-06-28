use dynamic_graphql::InputObject;

#[derive(InputObject)]
pub(crate) struct StringFilter {
    pub(crate) eq: Option<String>,
    pub(crate) ne: Option<String>,
}

impl StringFilter {
    pub(crate) fn matches(&self, value: &str) -> bool {
        if !self.eq.as_ref().map_or(true, |eq| value == eq) {
            return false;
        }
        self.ne.as_ref().map_or(true, |ne| value != ne)
    }
}

#[derive(InputObject)]
pub(crate) struct NumberFilter {
    gt: Option<usize>,
    lt: Option<usize>,
    eq: Option<usize>,
    ne: Option<usize>,
    gte: Option<usize>,
    lte: Option<usize>,
}

impl NumberFilter {
    pub(crate) fn matches(&self, value: usize) -> bool {
        if let Some(gt) = self.gt {
            if value <= gt {
                return false;
            }
        }

        if let Some(lt) = self.lt {
            if value >= lt {
                return false;
            }
        }

        if let Some(eq) = self.eq {
            if value != eq {
                return false;
            }
        }

        if let Some(ne) = self.ne {
            if value == ne {
                return false;
            }
        }

        if let Some(gte) = self.gte {
            if value < gte {
                return false;
            }
        }

        if let Some(lte) = self.lte {
            if value > lte {
                return false;
            }
        }

        true
    }
}
