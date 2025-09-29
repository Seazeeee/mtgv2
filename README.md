# MTGv2 Pipeline + Systems

The goal is for me to learn how to make a functional API via ASP.NET
utilizing modern-day security principals such as JWT, and learning more
about python classes and inheritance.

## 🐧 Note: WSL2 & Dagster gRPC Socket Issues

Dagster sometimes fails to launch code servers inside **WSL2** due to gRPC over Unix sockets.  
If you see errors like `UNAVAILABLE: GOAWAY received` or the code server immediately dying,  
you can work around this by manually starting the gRPC server and pointing Dagster at it.

**Steps:**

1. Start the gRPC server manually:

   ```bash
   dagster api grpc -p <PORT> -h 0.0.0.0 (localhost)
   ```

2. Create/update `workspace.yaml` to point to that server:

   ```yaml
   load_from:
     - grpc_server:
         host: "localhost"
         port: <PORT>
   ```

3. Run the Dagster web server as usual:

   ```bash
   dg dev --use-ssl -w workspace.yml
   ```
