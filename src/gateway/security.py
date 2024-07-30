import grpc
import jwt
import logging


class BearerTokenAuthInterceptor(grpc.ServerInterceptor):
    def __init__(self,
                 audience: str,
                 secret_key: str,
                 logger: logging.Logger
                 ):
        self.audience = audience
        self.secret_key = secret_key
        self.logger = logger

    def intercept_service(self, continuation, handler_call_details):
        # Extract metadata from the incoming request
        metadata = dict(handler_call_details.invocation_metadata)
        auth_header = metadata.get('authorization')

        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[len('Bearer '):]
            self.logger.debug(msg=f"Received token: {token}")

            try:
                # Validate the token
                decoded_token = jwt.decode(jwt=token,
                                           key=self.secret_key,
                                           verify=True,
                                           audience=self.audience,
                                           algorithms=['HS256']
                                           )
                # If we got this far, the token is valid
                self.logger.info(msg=f"Valid token for user: {decoded_token.get('sub')}")

                # Continue with the call
                return continuation(handler_call_details)
            except jwt.exceptions.ExpiredSignatureError:
                def unauthenticated_response(request, context):
                    context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                    context.set_details('Token has expired')
                    return None
                return grpc.unary_unary_rpc_method_handler(unauthenticated_response)
            except jwt.exceptions.DecodeError:
                def unauthenticated_response(request, context):
                    context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                    context.set_details(f'Invalid token')
                    return None
                return grpc.unary_unary_rpc_method_handler(unauthenticated_response)
        else:
            def unauthenticated_response(request, context):
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('No valid bearer token')
                return None
            return grpc.unary_unary_rpc_method_handler(unauthenticated_response)
