import { SignatureResult, TransactionInstruction } from "@solana/web3.js";
import BN from "bn.js";
import { Address } from "components/common/Address";
import { useCluster } from "providers/cluster";
import { useEffect, useState } from "react";
import { InstructionCard } from "../InstructionCard";
import {
  baseLotsToNumber,
  getPerpMarketFromInstruction,
  getPerpMarketFromPerpMarketConfig,
  OrderLotDetails,
  PlacePerpOrder,
  priceLotsToNumber,
} from "./types";

export function PlacePerpOrderDetailsCard(props: {
  ix: TransactionInstruction;
  index: number;
  result: SignatureResult;
  info: PlacePerpOrder;
  innerCards?: JSX.Element[];
  childIndex?: number;
}) {
  const { ix, index, result, info, innerCards, childIndex } = props;
  const mangoAccount = ix.keys[1];
  const perpMarketAccountMeta = ix.keys[4];
  const mangoPerpMarketConfig = getPerpMarketFromInstruction(
    ix,
    perpMarketAccountMeta
  );

  const cluster = useCluster();
  const [orderLotDetails, setOrderLotDetails] =
    useState<OrderLotDetails | null>(null);
  useEffect(() => {
    async function getOrderLotDetails() {
      if (mangoPerpMarketConfig === undefined) {
        return;
      }
      const mangoPerpMarket = await getPerpMarketFromPerpMarketConfig(
        cluster.url,
        mangoPerpMarketConfig
      );
      
      let limitPrice = 0;
      let maxBaseQuantity = 0;

      const quantity = new BN(info.quantity.toString());
      const price = new BN(info.price.toString());

      if (mangoPerpMarket) {
        maxBaseQuantity = mangoPerpMarket.baseLotsToNumber(quantity);
        limitPrice = mangoPerpMarket.priceLotsToNumber(price);
      } else {
        // Market has been delisted, fetch hardcoded info from config
        const delistedConfig = mangoPerpMarketConfig as any;
        maxBaseQuantity = baseLotsToNumber(
          quantity,
          delistedConfig["baseLotSize"],
          delistedConfig["baseDecimals"]
        );
        limitPrice = priceLotsToNumber(
          price,
          delistedConfig["baseLotSize"],
          delistedConfig["quoteLotSize"],
          delistedConfig["baseDecimals"],
          delistedConfig["quoteDecimals"]
        );
      }

      setOrderLotDetails({
        price: limitPrice,
        size: maxBaseQuantity,
      } as OrderLotDetails);
    }
    getOrderLotDetails();
  }, [cluster.url, info.quantity, info.price, mangoPerpMarketConfig]);

  return (
    <InstructionCard
      ix={ix}
      index={index}
      result={result}
      title="Mango Program: PlacePerpOrder"
      innerCards={innerCards}
      childIndex={childIndex}
    >
      <tr>
        <td>Mango account</td>
        <td>
          {" "}
          <Address pubkey={mangoAccount.pubkey} alignRight link />
        </td>
      </tr>

      {mangoPerpMarketConfig !== undefined && (
        <tr>
          <td>Perp market</td>
          <td className="text-lg-end">{mangoPerpMarketConfig.name}</td>
        </tr>
      )}

      <tr>
        <td>Perp market address</td>
        <td>
          <Address pubkey={perpMarketAccountMeta.pubkey} alignRight link />
        </td>
      </tr>

      {info.clientOrderId !== "0" && (
        <tr>
          <td>Client order Id</td>
          <td className="text-lg-end">{info.clientOrderId}</td>
        </tr>
      )}

      <tr>
        <td>Order type</td>
        <td className="text-lg-end">{info.orderType}</td>
      </tr>
      <tr>
        <td>side</td>
        <td className="text-lg-end">{info.side}</td>
      </tr>

      {orderLotDetails !== null && (
        <tr>
          <td>price</td>
          <td className="text-lg-end">{orderLotDetails?.price} USDC</td>
        </tr>
      )}

      {orderLotDetails !== null && (
        <tr>
          <td>quantity</td>
          <td className="text-lg-end">{orderLotDetails?.size}</td>
        </tr>
      )}
      <tr>
        <td>Reduce only</td>
        <td className="text-lg-end">{info.reduceOnly}</td>
      </tr>
    </InstructionCard>
  );
}
