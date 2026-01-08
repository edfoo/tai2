from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

try:  # pragma: no cover - optional dependency wiring
    import okx.Account as OkxAccount
    import okx.Trade as OkxTrade
    import okx.utils as OkxUtils
    from okx.consts import ACCOUNT_INFO, GET, PLACR_ORDER, POSITION_INFO, POST
except ImportError:  # pragma: no cover - runtime fallback
    OkxAccount = None
    OkxTrade = None
    OkxUtils = None
    ACCOUNT_INFO = None
    POSITION_INFO = None
    PLACR_ORDER = None
    GET = None
    POST = None

def _ensure_okx_utils() -> None:
    if not OkxUtils:
        return
    if not hasattr(OkxUtils, "get_timestamp"):
        def _default_timestamp() -> str:
            now = datetime.now(timezone.utc)
            return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")

        OkxUtils.get_timestamp = _default_timestamp  # type: ignore[attr-defined]

    if getattr(OkxUtils, "_tai2_header_patch_applied", False):
        return

    original_get_header = getattr(OkxUtils, "get_header", None)
    original_get_header_no_sign = getattr(OkxUtils, "get_header_no_sign", None)

    def _normalize_sim_flag(flag: Any) -> bool:
        if isinstance(flag, str):
            return flag.strip() == "1"
        if isinstance(flag, (int, float)):
            return int(flag) == 1
        return bool(flag)

    if original_get_header:
        def _patched_get_header(api_key, signature, timestamp, pass_phrase, simulation, debug):
            return original_get_header(
                api_key,
                signature,
                timestamp,
                pass_phrase,
                _normalize_sim_flag(simulation),
                debug,
            )

        OkxUtils.get_header = _patched_get_header  # type: ignore[attr-defined]

    if original_get_header_no_sign:
        def _patched_get_header_no_sign(simulation, debug):
            return original_get_header_no_sign(_normalize_sim_flag(simulation), debug)

        OkxUtils.get_header_no_sign = _patched_get_header_no_sign  # type: ignore[attr-defined]

    OkxUtils._tai2_header_patch_applied = True  # type: ignore[attr-defined]
    # Removed reassignment of GET and POST here


_ensure_okx_utils()


@dataclass(slots=True)
class OkxSdkClients:
    """Container for hydrated okx-sdk REST clients."""

    account: Any | None
    trade: Any | None


class OkxAccountAdapter:
    """Adds sub-account awareness to the okx-sdk AccountAPI."""

    def __init__(self, account_api: Any | None) -> None:
        self._api = account_api

    def get_positions(
        self,
        instType: str = "",
        instId: str = "",
        posId: str = "",
        subAcct: str | None = None,
    ) -> Any:
        if not self._api:
            return []
        if subAcct and hasattr(self._api, "_request_with_params") and POSITION_INFO and GET:
            params = {
                "instType": instType,
                "instId": instId,
                "posId": posId,
                "subAcct": subAcct,
            }
            return self._api._request_with_params(GET, POSITION_INFO, params)
        return self._api.get_positions(instType=instType, instId=instId, posId=posId)

    def get_account_balance(self, ccy: str = "", subAcct: str | None = None) -> Any:
        if not self._api:
            return {}
        if subAcct and hasattr(self._api, "_request_with_params") and ACCOUNT_INFO and GET:
            params = {"ccy": ccy, "subAcct": subAcct}
            return self._api._request_with_params(GET, ACCOUNT_INFO, params)
        return self._api.get_account_balance(ccy=ccy)


class OkxTradeAdapter:
    """Injects sub-account routing into okx-sdk TradeAPI order placement."""

    def __init__(self, trade_api: Any | None) -> None:
        self._api = trade_api

    def place_order(
        self,
        instId,
        tdMode,
        side,
        ordType,
        sz,
        ccy="",
        clOrdId="",
        tag="",
        posSide="",
        px="",
        reduceOnly="",
        tgtCcy="",
        stpMode="",
        attachAlgoOrds=None,
        pxUsd="",
        pxVol="",
        banAmend="",
        subAcct: str | None = None,
    ) -> Any:
        if not self._api:
            raise RuntimeError("Trade API unavailable")
        if subAcct and hasattr(self._api, "_request_with_params") and PLACR_ORDER and POST:
            params = {
                "instId": instId,
                "tdMode": tdMode,
                "side": side,
                "ordType": ordType,
                "sz": sz,
                "ccy": ccy,
                "clOrdId": clOrdId,
                "tag": tag,
                "posSide": posSide,
                "px": px,
                "reduceOnly": reduceOnly,
                "tgtCcy": tgtCcy,
                "stpMode": stpMode,
                "pxUsd": pxUsd,
                "pxVol": pxVol,
                "banAmend": banAmend,
                "subAcct": subAcct,
            }
            params["attachAlgoOrds"] = attachAlgoOrds
            return self._api._request_with_params(POST, PLACR_ORDER, params)
        return self._api.place_order(
            instId=instId,
            tdMode=tdMode,
            side=side,
            ordType=ordType,
            sz=sz,
            ccy=ccy,
            clOrdId=clOrdId,
            tag=tag,
            posSide=posSide,
            px=px,
            reduceOnly=reduceOnly,
            tgtCcy=tgtCcy,
            stpMode=stpMode,
            attachAlgoOrds=attachAlgoOrds,
            pxUsd=pxUsd,
            pxVol=pxVol,
            banAmend=banAmend,
        )


def build_okx_sdk_clients(
    *,
    api_key: str | None,
    api_secret: str | None,
    passphrase: str | None,
    flag: str = "0",
) -> OkxSdkClients:
    has_credentials = bool(api_key and api_secret and passphrase)
    account = None
    trade = None
    if OkxAccount is not None and has_credentials:
        account = OkxAccount.AccountAPI(
            api_key=api_key,
            api_secret_key=api_secret,
            passphrase=passphrase,
            flag=flag,
        )
    if OkxTrade is not None and has_credentials:
        trade = OkxTrade.TradeAPI(
            api_key=api_key,
            api_secret_key=api_secret,
            passphrase=passphrase,
            flag=flag,
        )
    return OkxSdkClients(account=account, trade=trade)
