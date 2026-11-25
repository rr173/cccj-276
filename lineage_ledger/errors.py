class LedgerError(Exception):
    """Base class for ledger failures."""


class ValidationRejected(LedgerError):
    """The submitted event is malformed or violates envelope rules."""


class ConflictRejected(LedgerError):
    """A stable identity/version/sequence was reused with different content."""


class PersistenceClosed(LedgerError):
    """Operation attempted after the ledger connection was closed."""
