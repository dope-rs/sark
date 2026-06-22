use super::spec::RouteSpec;
use crate::{Request, request};

pub trait Route<State>: RouteSpec {
    fn invoke<'req, 'a>(
        &'a self,
        params: <Self as RouteSpec>::Params<'req>,
        req: &request::Ref<'req, <Self as RouteSpec>::Headers<'req>>,
        headers: <Self as RouteSpec>::Headers<'req>,
        parsed_body: <Self as RouteSpec>::ParsedBody<'req>,
        state: &'a State,
    ) -> <Self as RouteSpec>::Response<'req>
    where
        'req: 'a;
}

pub trait StreamRoute<State>: RouteSpec {
    fn invoke(
        &self,
        params: <Self as RouteSpec>::Params<'static>,
        req: Request,
        headers: <Self as RouteSpec>::Headers<'static>,
        parsed_body: <Self as RouteSpec>::ParsedBody<'static>,
        state: &State,
    ) -> <Self as RouteSpec>::Response<'static>;
}
