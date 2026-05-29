#!/usr/bin/env python3
"""
Fails over a Temporal Cloud namespace to a target region using FailoverNamespaceRegion,
then polls GetAsyncOperation with the returned async_operation_id until the operation
reaches a terminal state.

Flow:
  1. GetNamespace (cloud API) — fetch current active_region and verify target region is a replica
  2. FailoverNamespaceRegion (cloud API) — request failover to the target region
  3. GetAsyncOperation (cloud API) — poll the async_operation_id until terminal state
"""

import asyncio
import os
import sys
import time

from temporalio.api.cloud.cloudservice.v1 import (
    FailoverNamespaceRegionRequest,
    GetAsyncOperationRequest,
    GetNamespaceRequest,
)
from temporalio.api.cloud.operation.v1 import AsyncOperation
from temporalio.client import CloudOperationsClient

CLOUD_API_VERSION = "v0.14.0"


async def failover_namespace(
    api_key: str,
    namespace_name: str,
    region: str,
) -> None:
    cloud_client = await CloudOperationsClient.connect(
        api_key=api_key,
        rpc_metadata={"temporal-cloud-api-version": CLOUD_API_VERSION},
    )
    cloud_service = cloud_client.cloud_service

    # Step 1: Fetch the current namespace to see active_region and configured regions.
    print(f"Fetching namespace '{namespace_name}' ...")
    get_resp = await cloud_service.get_namespace(
        GetNamespaceRequest(namespace=namespace_name)
    )
    ns = get_resp.namespace
    active_region = ns.active_region
    configured_regions = list(ns.spec.regions)
    print(f"  Active region     : {active_region}")
    print(f"  Configured regions: {configured_regions}")

    if region not in configured_regions:
        print(
            f"ERROR: Region '{region}' is not configured on namespace "
            f"'{namespace_name}'. Add it as a replica first."
        )
        sys.exit(1)

    if active_region == region:
        print(f"Namespace is already active in '{region}' — nothing to do.")
        return

    # Step 2: Submit FailoverNamespaceRegion via the cloud API.
    print(f"\nSubmitting FailoverNamespaceRegion to '{region}' ...")
    submit_start = time.monotonic()
    failover_resp = await cloud_service.failover_namespace_region(
        FailoverNamespaceRegionRequest(
            namespace=namespace_name,
            region=region,
        )
    )
    operation_id = failover_resp.async_operation.id
    print(f"FailoverNamespaceRegion accepted. async_operation_id={operation_id}")

    # Step 3: Poll GetAsyncOperation until the operation reaches a terminal state.
    poll_interval = 5
    max_poll_interval = 10
    terminal_states = {
        AsyncOperation.STATE_FAILED,
        AsyncOperation.STATE_CANCELLED,
        AsyncOperation.STATE_FULFILLED,
        AsyncOperation.STATE_REJECTED,
    }
    print("\nPolling GetAsyncOperation until terminal state ...")

    while True:
        await asyncio.sleep(poll_interval)
        op_resp = await cloud_service.get_async_operation(
            GetAsyncOperationRequest(async_operation_id=operation_id)
        )
        op = op_resp.async_operation
        state_name = AsyncOperation.State.Name(op.state)
        print(f"  state={state_name}")

        if op.state in terminal_states:
            elapsed = time.monotonic() - submit_start
            if op.state == AsyncOperation.STATE_FULFILLED:
                print(
                    f"\nNamespace '{namespace_name}' is now active in '{region}'. "
                    f"Failover completed in {elapsed:.2f}s."
                )
                return
            print(
                f"\nFailover did not succeed after {elapsed:.2f}s. "
                f"state={state_name} failure_reason={op.failure_reason!r}"
            )
            sys.exit(1)

        poll_interval = min(poll_interval * 2, max_poll_interval)


if __name__ == "__main__":
    api_key = os.environ.get("TEMPORAL_CLOUD_API_KEY")
    if not api_key:
        print("ERROR: Set the TEMPORAL_CLOUD_API_KEY environment variable.")
        sys.exit(1)

    namespace = os.environ.get("TEMPORAL_NAMESPACE", "gaurav-mrn-ops-sample.a2dd6")
    region = os.environ.get("TEMPORAL_FAILOVER_REGION", "gcp-us-central1")

    asyncio.run(
        failover_namespace(
            api_key=api_key,
            namespace_name=namespace,
            region=region,
        )
    )
