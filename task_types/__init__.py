"""Task type handlers."""

from .message import handle_message
from .poll import handle_poll
from .query_for_update import handle_query_for_update

__all__ = ['handle_message', 'handle_poll', 'handle_query_for_update']
