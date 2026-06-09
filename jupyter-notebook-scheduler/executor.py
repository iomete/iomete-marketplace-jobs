import asyncio
import logging
import os
import threading
from urllib.parse import urlparse

import nbformat
import websocket
from jupyter_server.gateway.gateway_client import GatewayClient, gateway_request
from jupyter_server.gateway.managers import GatewayKernelManager, GatewayKernelClient
from jupyter_server.utils import url_path_join, url_escape
from jupyter_core.utils import ensure_async
from nbclient import NotebookClient

logger = logging.getLogger(__name__)


class NotebookExecutor:
    """Executes a notebook remotely against an IOMETE Jupyter Container.

    Rather than spinning up a local kernel, the notebook cells are executed on a
    remote Spark-backed kernel provisioned by the container. The executed notebook
    (including outputs) is written back to disk for the caller to upload.
    """

    def __init__(self, config, working_dir):
        self.config = config
        self.working_dir = working_dir
        self._configure_gateway()

    def _configure_gateway(self):
        gw = GatewayClient.instance()
        gw.url = self.config.gateway_url
        gw.auth_token = self.config.gateway_token
        gw.auth_scheme = self.config.gateway_auth_scheme
        gw.accept_cookies = True
        gw.request_timeout = self.config.gateway_request_timeout
        gw.connect_timeout = self.config.gateway_connect_timeout

    def get_output_path(self):
        """Returns the expected output path for the executed notebook."""
        input_path = os.path.join(self.working_dir, self.config.main_notebook_file)
        notebook_dir = os.path.dirname(input_path)
        notebook_name = os.path.basename(input_path)
        return os.path.join(notebook_dir, f"{notebook_name}")

    def execute(self):
        """Executes the notebook on the remote gateway.

        The executed notebook is always written to the output path -- including
        on failure -- so partial results can be uploaded for debugging. Returns
        the path to the executed notebook file.
        """
        input_path = os.path.join(self.working_dir, self.config.main_notebook_file)
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Notebook file not found: {input_path}")

        output_path = self.get_output_path()

        logger.info(f"Reading notebook: {input_path}")
        nb = nbformat.read(input_path, as_version=4)

        if self.config.notebook_params:
            self._inject_parameters(nb, self.config.notebook_params)

        logger.info(f"Priming gateway session: {self.config.gateway_url}")
        asyncio.run(self._prime_cookies())

        logger.info(
            f"Executing notebook on gateway (kernel={self.config.gateway_kernel_name}, "
            f"timeout={self.config.gateway_execution_timeout}s)"
        )
        try:
            NotebookClient(
                nb,
                kernel_name=self.config.gateway_kernel_name,
                kernel_manager_class=XsrfAwareGatewayKernelManager,
                timeout=self.config.gateway_execution_timeout,
            ).execute()
            logger.info("Notebook execution completed successfully")
        finally:
            # Persist whatever we have -- on success the full notebook, on
            # failure the partially-executed notebook (useful for debugging).
            nbformat.write(nb, output_path)
            logger.info(f"Saved executed notebook to: {output_path}")

        return output_path

    def _inject_parameters(self, nb, params):
        """Injects a parameters cell, papermill-style.

        Inserts a code cell that assigns the configured parameters. If a cell
        tagged ``parameters`` exists, the injected cell is placed immediately
        after it (overriding defaults); otherwise it is placed at the top.
        """
        assignments = "\n".join(f"{key} = {value!r}" for key, value in params.items())
        source = "# Injected parameters\n" + assignments
        param_cell = nbformat.v4.new_code_cell(source=source)
        param_cell.metadata["tags"] = ["injected-parameters"]

        insert_at = 0
        for index, cell in enumerate(nb.cells):
            if "parameters" in cell.get("metadata", {}).get("tags", []):
                insert_at = index + 1
                break

        nb.cells.insert(insert_at, param_cell)
        logger.info(f"Injected {len(params)} parameter(s) at cell index {insert_at}")

    async def _prime_cookies(self):
        """Performs an authenticated GET so the gateway issues session cookies.

        The websocket upgrade for the kernel channel requires the same cookies
        (and XSRF token) the gateway hands out on a regular HTTP request.
        """
        gw = GatewayClient.instance()
        url = url_path_join(gw.url, gw.kernelspecs_endpoint)
        await gateway_request(url, method="GET")
        logger.debug(f"Gateway cookies primed: {list(gw._cookies)}")


###############################################################################
# Helper classes for auth + cookies on the websocket kernel channel.
#
# The stock GatewayKernelClient does not forward session cookies or the XSRF
# token on the websocket upgrade, which the IOMETE gateway requires. These
# subclasses build the upgrade request with the necessary headers.
###############################################################################


class XsrfAwareGatewayKernelClient(GatewayKernelClient):
    async def start_channels(self, shell=True, iopub=True, stdin=True, hb=True, control=True):
        gw = GatewayClient.instance()

        ws_url = url_path_join(
            gw.ws_url or "",
            gw.kernels_endpoint,
            url_escape(self.kernel_id),
            "channels",
        )

        # Build cookie header from whatever GatewayClient has stored.
        cookie_header = "; ".join(
            f"{name}={morsel.coded_value}" for name, (morsel, _) in gw._cookies.items()
        )

        # Origin must match the server origin or Jupyter rejects the upgrade.
        parsed = urlparse(gw.url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        headers = [f"Authorization: {gw.auth_scheme} {gw.auth_token}"]
        if cookie_header:
            headers.append(f"Cookie: {cookie_header}")
        # Some Jupyter setups also check X-XSRFToken on the upgrade.
        xsrf = gw._cookies.get("_xsrf")
        if xsrf:
            headers.append(f"X-XSRFToken: {xsrf[0].coded_value}")

        ssl_options = {
            "ca_certs": gw.ca_certs,
            "certfile": gw.client_cert,
            "keyfile": gw.client_key,
        }

        self.channel_socket = websocket.create_connection(
            ws_url,
            timeout=gw.KERNEL_LAUNCH_TIMEOUT,
            enable_multithread=True,
            sslopt=ssl_options,
            header=headers,
            origin=origin,
        )

        from jupyter_client.asynchronous.client import AsyncKernelClient
        await ensure_async(
            AsyncKernelClient.start_channels(
                self, shell=shell, iopub=iopub, stdin=stdin, hb=hb, control=control
            )
        )
        self.response_router = threading.Thread(target=self._route_responses)
        self.response_router.start()


class XsrfAwareGatewayKernelManager(GatewayKernelManager):
    # Fully-qualified path so traitlets can import the client outside __main__.
    client_class = f"{__name__}.XsrfAwareGatewayKernelClient"

    def client(self, **kwargs):
        kw = {}
        kw.update(self.get_connection_info(session=True))
        kw.update({"connection_file": self.connection_file, "parent": self})
        kw["kernel_id"] = self.kernel_id
        kw.update(kwargs)
        return XsrfAwareGatewayKernelClient(**kw)
