//! Named subjects with request-local acting roles.

use alloc::borrow::Cow;
use alloc::string::String;

use super::deployment;
use crate::visibility::policy::{RequestValues, Subject};

/// A subject whose name may be borrowed or owned.
#[derive(Clone, Debug)]
pub struct Actor<N = String> {
    /// The authorization subject name.
    pub name: N,
    /// The role stated by this request.
    pub role: Option<&'static str>,
}

impl Actor<String> {
    /// Construct a subject acting as the deployment's role.
    #[must_use]
    pub fn app_user(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            role: Some(deployment::ROLE),
        }
    }
}

impl<N: AsRef<str>> Subject for Actor<N> {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        core::iter::once(Cow::Borrowed(self.name.as_ref()))
    }

    fn acting_role(&self) -> Option<&str> {
        self.role
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        if parameter != "app_user_id" {
            return false;
        }
        let Some(identity) = self.name.as_ref().rsplit_once(':').map(|(_, id)| id) else {
            return false;
        };
        out.push(identity);
        true
    }
}
