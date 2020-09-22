use std::marker::PhantomData;

/// A streaming iter is one where each call to next returns a reference to an item (that borrows from self).
/// This is needed in many cases to allow us to reuse buffers/objects etc for performance reasons.
/// This particular implementation of streaming iterator actually returns result<option> for next
/// this avoids a bunch of wrapping/unwrapping or result<option<result>>'s that we end up having to
/// deal with when iterating from sources like files where errors may occur.
/// This makes this iterator perfect for data processing type workloads
pub trait StreamingIter {
    type I: ?Sized;
    type E;
    /// Advance the iterator to the next position, should be called before get for a new iter
    fn advance(&mut self) -> Result<(), Self::E>;

    /// Get the data at the current position of the iterator
    fn get(&self) -> Option<&Self::I>;

    /// Short cut function that calls advance followed by get.
    fn next(&mut self) -> Result<Option<&Self::I>, Self::E> {
        self.advance()?;
        Ok(self.get())
    }
}

/// Returns an empty iter
pub fn empty<I: ?Sized, E>() -> EmptyIter<I, E> {
    EmptyIter {
        _p: PhantomData::default(),
    }
}

/// Returns an streaming iter wrapper around a standard iterator that returns a reference
pub fn wrap<'a, IT, I: ?Sized + 'a, E>(iter: IT) -> WrappingIter<'a, IT, I, E>
where
    IT: Iterator<Item = &'a I>,
{
    WrappingIter {
        inner: iter,
        _p: PhantomData::default(),
        item: None,
    }
}

/// An empty iterator
pub struct EmptyIter<I: ?Sized, E> {
    _p: PhantomData<(E, I)>,
}

impl<I: ?Sized, E> StreamingIter for EmptyIter<I, E> {
    type I = I;
    type E = E;
    fn advance(&mut self) -> Result<(), Self::E> {
        Ok(())
    }

    fn get(&self) -> Option<&Self::I> {
        None
    }
}

/// An streaming iterator that wraps a standard iter returning a reference.
pub struct WrappingIter<'a, IT: Iterator, I: ?Sized + 'a, E>
where
    IT: Iterator<Item = &'a I>,
{
    inner: IT,
    _p: PhantomData<(E, I)>,
    item: Option<&'a I>,
}

impl<'a, I: ?Sized + 'a, IT, E> StreamingIter for WrappingIter<'a, IT, I, E>
where
    IT: Iterator<Item = &'a I>,
{
    type I = I;
    type E = E;
    fn advance(&mut self) -> Result<(), Self::E> {
        self.item = self.inner.next();
        Ok(())
    }

    fn get(&self) -> Option<&Self::I> {
        self.item
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let mut e = empty::<i32, ()>();
        assert_eq!(e.next().unwrap(), None);
    }

    #[test]
    fn test_wrapping() {
        let inner_iter = [1, 2, 3].iter();
        let mut wrapper = wrap::<_, i32, ()>(inner_iter);
        assert_eq!(wrapper.next().unwrap(), Some(&1));
        assert_eq!(wrapper.next().unwrap(), Some(&2));
        assert_eq!(wrapper.next().unwrap(), Some(&3));
        assert_eq!(wrapper.next().unwrap(), None);
    }
}
