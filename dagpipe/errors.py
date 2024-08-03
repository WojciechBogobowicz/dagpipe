class ArgumentError(Exception):
    """Custom exception for argument errors."""


class EmptyInputError(Exception):
    """Custom exception for empty input errors."""


class UndefinedTaskIndexAccessError(IndexError):
    """User is trying to access to task reference that wasn't defined."""
