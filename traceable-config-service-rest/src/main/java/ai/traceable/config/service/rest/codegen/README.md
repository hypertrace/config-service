# Code generation of JAX-RS Resources

## Why?
- The rpc definition is already provided in the protobuf, which is the service contract.
- To avoid "re-definition" of this contract and help make the rpc layer automatically accessible over REST.

## How?
- Consider how we do this manually:
  - To expose rpc over REST, we must define a JAX-RS resource that reflect the methods the rpc definition.
  - Within this resource, we provide a stub that can communicate with the server that provides the rpc implementation over a channel. 
  - When a REST endpoint is invoked - 
    - the request context is provided by the context provider
    - the request payload is deserialized, typically from JSON, to protobuf
    - rpc method is invoked using the stub
    - the response payload is serialized from protobuf to JSON.
- To achieve this dynamically:
- JaxRsResourceCreator: Core class responsible for generating a JAX-RS resource, given the rpc definition.
- SimpleGrpcInterceptor: A simple interceptor bound to the generated resources, that's responsible for invoking the rpc server over the given channel
- GrpcStubGenerator: Instantiates rpc stub using reflection
- JaxRsResourceGenerator: Entry point to the code generation, which generates JAX-RS resources for a given config.
