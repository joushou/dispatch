dispatch
========

Work dispatcher. It consists of of a managing server that hosts all available modules, and a control component running on all the slaves.
The control component uses a meta_path hook to catch all imports and query the server.
Dispatch works by issuing a dispatch request to the server with the wanted target machine and module name, which will then result in a spawned process on the slave that starts out by loading the requested module.

No code-changes should be necessary. Due to the bytecode-serializing nature of dispatch, a slave needs to run the same Python implementation as the module creator.
I might change or fix this in the future.

**Dependencies**
* Stackable
* Runnable

I'll try to eliminate the Runnable dependency in the near future - It only relies on the server-component of it.