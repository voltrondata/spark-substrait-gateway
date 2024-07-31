# SPDX-License-Identifier: Apache-2.0
"""A gRPC interceptor that validates bearer tokens."""
import logging

import grpc
import jwt


class BearerTokenAuthInterceptor(grpc.ServerInterceptor):
    """A gRPC interceptor that validates bearer tokens."""

    def __init__(self,
                 audience: str,
                 secret_key: str,
                 logger: logging.Logger
                 ):
        """Initialize the BearerTokenAuthInterceptor."""
        self.audience = audience
        self.secret_key = secret_key
        self.logger = logger

    def intercept_service(self, continuation, handler_call_details):
        """Intercept the incoming request and validates the bearer token."""
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

                return grpc.unary_unary_rpc_method_handler(unauthenticated_response)
            except jwt.exceptions.DecodeError:
                def unauthenticated_response(request, context):
                    context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                    context.set_details('Invalid token')

                return grpc.unary_unary_rpc_method_handler(unauthenticated_response)
        else:
            def unauthenticated_response(request, context):
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('No valid bearer token')

            return grpc.unary_unary_rpc_method_handler(unauthenticated_response)
