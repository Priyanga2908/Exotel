import { Command as $Command } from "@smithy/core/client";
import { MetadataBearer as __MetadataBearer } from "@smithy/types";
import {
  BedrockRuntimeClientResolvedConfig,
  ServiceInputTypes,
  ServiceOutputTypes,
} from "../BedrockRuntimeClient";
import {
  InvokeGuardrailChecksRequest,
  InvokeGuardrailChecksResponse,
} from "../models/models_0";
export { __MetadataBearer };
export { $Command };
export interface InvokeGuardrailChecksCommandInput
  extends InvokeGuardrailChecksRequest {}
export interface InvokeGuardrailChecksCommandOutput
  extends InvokeGuardrailChecksResponse,
    __MetadataBearer {}
declare const InvokeGuardrailChecksCommand_base: {
  new (
    input: InvokeGuardrailChecksCommandInput
  ): import("@smithy/core/client").CommandImpl<
    InvokeGuardrailChecksCommandInput,
    InvokeGuardrailChecksCommandOutput,
    BedrockRuntimeClientResolvedConfig,
    ServiceInputTypes,
    ServiceOutputTypes
  >;
  new (
    input: InvokeGuardrailChecksCommandInput
  ): import("@smithy/core/client").CommandImpl<
    InvokeGuardrailChecksCommandInput,
    InvokeGuardrailChecksCommandOutput,
    BedrockRuntimeClientResolvedConfig,
    ServiceInputTypes,
    ServiceOutputTypes
  >;
  getEndpointParameterInstructions(): {
    [x: string]: unknown;
  };
};
export declare class InvokeGuardrailChecksCommand extends InvokeGuardrailChecksCommand_base {
  protected static __types: {
    api: {
      input: InvokeGuardrailChecksRequest;
      output: InvokeGuardrailChecksResponse;
    };
    sdk: {
      input: InvokeGuardrailChecksCommandInput;
      output: InvokeGuardrailChecksCommandOutput;
    };
  };
}
