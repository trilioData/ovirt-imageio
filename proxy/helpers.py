from functools import wraps
import auth



def requiresession(func):
    """
    Annotation to wrap an HTTP method to ensure a session is loaded, returning
    a failed HTTP response if one could not be established.

    :param func: HTTP method to wrap
    :return: webob response object, either failure (if session could not be
             initialized) or HTTP method return value
    :raise webob.exc.HTTPException: Error creating session or running HTTP
                                    method
    """
    @wraps(func)
    def wrapper(self, *args):
        auth.start_session(self.request)
        try:
            ret = func(self, *args)
        finally:
            auth.update_session_activity(self.request)
        return ret
    return wrapper