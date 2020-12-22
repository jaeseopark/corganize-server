## Local

```bash
aws configure --profile corganize-server
# ... setup Credenetials

# Run the server without a debugger
sam local start-api --profile corganize-server
```

### Debugging with a debugger

See: https://blog.thundra.io/debug-your-python-functions-locally

TLDR:

1. Install ptvsd through pip and add the following at the beginnig of the app

```python
import ptvsd
ptvsd.enable_attach(address=("0.0.0.0", 5890), redirect_output=True)
ptvsd.wait_for_attach()
```

2. Place a break point
3. Start the server and hit it with a postman request
4. (Alternatively to Step 2) Use the `invoke` command of `sam`
5. Hit the debug button in VS Code
