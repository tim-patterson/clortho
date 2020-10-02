use std::marker::PhantomData;

/// A streaming iter is one where each call to next returns a reference to an item (that borrows from self).
/// This is needed in many cases to allow us to reuse buffers/objects etc for performance reasons.
/// This particular implementation of streaming iterator actually returns result<option> for next
/// this avoids a bunch of wrapping/unwrapping or result<option<result>>'s that we end up having to
/// deal with when iterating from sources like files where errors may occur.
/// This makes this iterator perfect for data processing type workloads.
/// Because we past by reference, its difficult to compose kv pairs as &(K, V) so we support KV as
/// a top level concept to instead support passes kv pairs as (&K, &V).
///
/// Really this iterator is specialized for use with a key value store....
pub trait StreamingKVIter {
    type K: ?Sized;
    type V: ?Sized;
    type E;

    /// Reposition the current position of the iterator.
    fn seek(&mut self, key: &Self::K) -> Result<(), Self::E>;

    /// Advance the iterator to the next position, should be called before get for a new iter
    fn advance(&mut self) -> Result<(), Self::E>;

    /// Get the data at the current position of the iterator
    fn get(&self) -> Option<(&Self::K, &Self::V)>;

    /// Short cut function that calls advance followed by get.
    #[allow(clippy::type_complexity)]
    fn next(&mut self) -> Result<Option<(&Self::K, &Self::V)>, Self::E> {
        self.advance()?;
        Ok(self.get())
    }
}

/// Returns an empty iter
pub fn empty<K: ?Sized, V: ?Sized, E>() -> EmptyIter<K, V, E> {
    EmptyIter {
        _p1: PhantomData::default(),
        _p2: PhantomData::default(),
    }
}

/// Returns an streaming iter wrapper around a standard iterator that returns a reference
pub fn wrap<'a, IT, K: ?Sized + 'a, V: ?Sized + 'a, E>(iter: IT) -> WrappingIter<'a, IT, K, V, E>
where
    IT: Iterator<Item = (&'a K, &'a V)>,
{
    WrappingIter {
        inner: iter,
        _p: PhantomData::default(),
        item: None,
    }
}

/// An empty iterator
pub struct EmptyIter<K: ?Sized, V: ?Sized, E> {
    _p1: PhantomData<(E, K)>,
    _p2: PhantomData<V>,
}

impl<K: ?Sized, V: ?Sized, E> StreamingKVIter for EmptyIter<K, V, E> {
    type K = K;
    type V = V;
    type E = E;
    fn seek(&mut self, _key: &Self::K) -> Result<(), Self::E> {
        Ok(())
    }

    fn advance(&mut self) -> Result<(), Self::E> {
        Ok(())
    }

    fn get(&self) -> Option<(&Self::K, &Self::V)> {
        None
    }
}

/// An streaming iterator that wraps a standard iter returning a reference.
/// Mostly used for tests...
pub struct WrappingIter<'a, IT: Iterator, K: ?Sized + 'a, V: ?Sized + 'a, E>
where
    IT: Iterator<Item = (&'a K, &'a V)>,
{
    inner: IT,
    _p: PhantomData<E>,
    item: Option<(&'a K, &'a V)>,
}

impl<'a, K: ?Sized + 'a, V: ?Sized + 'a, IT, E> StreamingKVIter for WrappingIter<'a, IT, K, V, E>
where
    IT: Iterator<Item = (&'a K, &'a V)>,
{
    type K = K;
    type V = V;
    type E = E;
    fn seek(&mut self, _key: &Self::K) -> Result<(), Self::E> {
        panic!()
    }

    fn advance(&mut self) -> Result<(), Self::E> {
        self.item = self.inner.next();
        Ok(())
    }

    fn get(&self) -> Option<(&Self::K, &Self::V)> {
        self.item
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let mut e = empty::<i32, i32, ()>();
        assert_eq!(e.next().unwrap(), None);
    }

    #[test]
    fn test_wrapping() {
        let inner_iter = vec![("a", "b"), ("c", "d"), ("e", "f")].into_iter();
        let mut wrapper = wrap::<_, _, _, ()>(inner_iter);
        assert_eq!(wrapper.next().unwrap(), Some(("a", "b")));
        assert_eq!(wrapper.next().unwrap(), Some(("c", "d")));
        assert_eq!(wrapper.next().unwrap(), Some(("e", "f")));
        assert_eq!(wrapper.next().unwrap(), None);
    }
}
