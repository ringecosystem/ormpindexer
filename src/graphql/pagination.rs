use sqlx::{Postgres, QueryBuilder};

pub(super) const DEFAULT_PAGE_LIMIT: i32 = 50;

pub(super) fn page_args(offset: Option<i32>, limit: Option<i32>) -> (i32, i32) {
    (
        offset.unwrap_or_default().max(0),
        limit.unwrap_or(DEFAULT_PAGE_LIMIT).max(0),
    )
}

pub(super) fn push_page(query: &mut QueryBuilder<'_, Postgres>, offset: i32, limit: i32) {
    query.push(" LIMIT ").push_bind(limit);
    query.push(" OFFSET ").push_bind(offset);
}
