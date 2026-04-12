#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional at import time
    load_dotenv = None


ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

DEFAULT_CREATION_DATE = "2024-06-25"
DEFAULT_API_VERSION = "v21.0"
WINDOW_DAYS = 30

TOTAL_METRICS = [
    "website_clicks",
    "profile_views",
    "accounts_engaged",
    "total_interactions",
    "likes",
    "comments",
    "shares",
    "saves",
    "replies",
    "follows_and_unfollows",
]

REELS_METRICS = [
    "views",
    "reach",
    "likes",
    "comments",
    "saved",
    "shares",
    "total_interactions",
    "ig_reels_avg_watch_time",
    "ig_reels_video_view_total_time",
]

FEED_METRICS = [
    "reach",
    "views",
    "likes",
    "comments",
    "saved",
    "shares",
    "total_interactions",
    "profile_activity",
    "profile_visits",
]

MEDIA_OUTPUT_COLUMNS = [
    "media_id",
    "media_type",
    "is_reel",
    "permalink",
    "timestamp",
    "caption",
    "status",
    "metrics_used",
    "metrics_dropped",
    "views",
    "reach",
    "likes",
    "comments",
    "saved",
    "shares",
    "total_interactions",
    "ig_reels_avg_watch_time",
    "ig_reels_video_view_total_time",
    "profile_activity",
    "profile_visits",
]


@dataclass
class Config:
    access_token: str
    ig_user_id: str
    creation_date: str
    api_version: str

    @property
    def base_url(self) -> str:
        return f"https://graph.facebook.com/{self.api_version}"


class InstagramPipelineError(RuntimeError):
    """Error controlado del pipeline con mensaje listo para usuario."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automatiza la extracción y preparación de datos de Instagram."
    )
    parser.add_argument(
        "command",
        choices=["extract", "transform", "all"],
        help="Paso a ejecutar del pipeline.",
    )
    parser.add_argument(
        "--skip-media",
        action="store_true",
        help="Omite la extracción de media insights.",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=50,
        help="Número de medias por página al consultar la API.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=50,
        help="Máximo de páginas al paginar medias.",
    )
    return parser.parse_args()


def load_config() -> Config:
    if load_dotenv is not None:
        load_dotenv(ROOT_DIR / ".env")

    access_token = clean_token(os.getenv("ACCESS_TOKEN") or os.getenv("USER_TOKEN"))
    ig_user_id = (os.getenv("IG_USER_ID") or "").strip()
    creation_date = (os.getenv("IG_CREATION_DATE") or DEFAULT_CREATION_DATE).strip()
    api_version = (os.getenv("IG_API_VERSION") or DEFAULT_API_VERSION).strip()

    missing = []
    if not access_token:
        missing.append("ACCESS_TOKEN")
    if not ig_user_id:
        missing.append("IG_USER_ID")

    if missing:
        raise SystemExit(
            "Faltan variables de entorno: "
            + ", ".join(missing)
            + ". Define esas variables en `.env` o en tu shell."
        )

    try:
        datetime.strptime(creation_date, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(
            "IG_CREATION_DATE debe tener formato YYYY-MM-DD."
        ) from exc

    return Config(
        access_token=access_token,
        ig_user_id=ig_user_id,
        creation_date=creation_date,
        api_version=api_version,
    )


def clean_token(token: str | None) -> str:
    return "".join(str(token or "").split())


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def fetch_json(url: str, *, params: dict, timeout: int = 60) -> dict:
    import requests

    response = requests.get(url, params=params, timeout=timeout)
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Respuesta no JSON de la API ({response.status_code}): {response.text[:300]}"
        ) from exc

    if response.status_code != 200:
        raise build_api_error(response.status_code, payload)

    return payload


def build_api_error(status_code: int, payload: dict) -> InstagramPipelineError:
    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    code = error.get("code")
    subcode = error.get("error_subcode")
    message = error.get("message") or str(payload)
    lowered = message.lower()

    if code in {190, 102} or "access token" in lowered or "session has expired" in lowered:
        return InstagramPipelineError(
            "El ACCESS_TOKEN ya no es valido o ha caducado. "
            "Actualiza la variable ACCESS_TOKEN en el archivo `.env` y vuelve a ejecutar el pipeline."
        )

    if code in {10, 200} or "permission" in lowered or "permissions error" in lowered:
        return InstagramPipelineError(
            "La API rechazo la peticion por permisos insuficientes. "
            "Revisa que el ACCESS_TOKEN tenga permisos para Instagram Graph API y que IG_USER_ID sea correcto."
        )

    if status_code == 400 and "unsupported get request" in lowered:
        return InstagramPipelineError(
            "La API no reconoce el recurso solicitado. "
            "Revisa la variable IG_USER_ID en `.env` y confirma que pertenece a la cuenta conectada."
        )

    detail = f"Graph API error {status_code}"
    if code is not None:
        detail += f" (code {code}"
        if subcode is not None:
            detail += f", subcode {subcode}"
        detail += ")"
    detail += f": {message}"
    return InstagramPipelineError(detail)


def totals_since_creation(config: Config) -> list[dict[str, object]]:
    url = f"{config.base_url}/{config.ig_user_id}/insights"
    totals = {metric: 0 for metric in TOTAL_METRICS}

    current = datetime.strptime(config.creation_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    end = datetime.now(timezone.utc)

    while current < end:
        nxt = min(current + timedelta(days=WINDOW_DAYS), end)
        payload = fetch_json(
            url,
            params={
                "metric": ",".join(TOTAL_METRICS),
                "period": "day",
                "metric_type": "total_value",
                "since": int(current.timestamp()),
                "until": int(nxt.timestamp()),
                "access_token": config.access_token,
            },
        )

        for item in payload.get("data", []):
            name = item.get("name")
            value = (item.get("total_value") or {}).get("value", 0)
            if name in totals and isinstance(value, (int, float)):
                totals[name] += value

        current = nxt

    return [{"metric": key, "value": value} for key, value in totals.items()]


def totals_by_window(config: Config) -> list[dict[str, object]]:
    url = f"{config.base_url}/{config.ig_user_id}/insights"
    rows: list[dict[str, object]] = []

    current = datetime.strptime(config.creation_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    end = datetime.now(timezone.utc)

    while current < end:
        nxt = min(current + timedelta(days=WINDOW_DAYS), end)
        payload = fetch_json(
            url,
            params={
                "metric": ",".join(TOTAL_METRICS),
                "period": "day",
                "metric_type": "total_value",
                "since": int(current.timestamp()),
                "until": int(nxt.timestamp()),
                "access_token": config.access_token,
            },
        )

        for item in payload.get("data", []):
            rows.append(
                {
                    "window_start": current.strftime("%Y-%m-%d"),
                    "window_end": nxt.strftime("%Y-%m-%d"),
                    "metric": item.get("name"),
                    "value": (item.get("total_value") or {}).get("value"),
                }
            )

        current = nxt

    return rows


def follower_demographics(config: Config, breakdown: str) -> list[dict[str, object]]:
    url = f"{config.base_url}/{config.ig_user_id}/insights"
    payload = fetch_json(
        url,
        params={
            "metric": "follower_demographics",
            "period": "lifetime",
            "metric_type": "total_value",
            "breakdown": breakdown,
            "access_token": config.access_token,
        },
    )

    rows: list[dict[str, object]] = []
    breakdowns = (
        payload.get("data", [{}])[0]
        .get("total_value", {})
        .get("breakdowns", [])
    )

    for item in breakdowns:
        if breakdown not in item.get("dimension_keys", []):
            continue
        for result in item.get("results", []):
            key = (result.get("dimension_values") or ["unknown"])[0]
            rows.append(
                {
                    "breakdown": breakdown,
                    "category": key,
                    "value": result.get("value", 0),
                }
            )
        break

    return rows


def get_media_list_all(
    config: Config,
    *,
    page_limit: int,
    max_pages: int,
    sleep_seconds: float = 0.2,
) -> list[dict]:
    url = f"{config.base_url}/{config.ig_user_id}/media"
    params: dict | None = {
        "fields": "id,media_type,caption,permalink,timestamp",
        "limit": page_limit,
        "access_token": config.access_token,
    }

    all_items: list[dict] = []
    pages = 0

    while url and pages < max_pages:
        payload = fetch_json(url, params=params or {})
        all_items.extend(payload.get("data", []))
        url = payload.get("paging", {}).get("next")
        params = None
        pages += 1
        time.sleep(sleep_seconds)

    return all_items


def insights_request(
    config: Config, media_id: str, metrics: list[str]
) -> tuple[int, dict]:
    import requests

    url = f"{config.base_url}/{media_id}/insights"
    response = requests.get(
        url,
        params={"metric": ",".join(metrics), "access_token": config.access_token},
        timeout=60,
    )
    try:
        payload = response.json()
    except ValueError:
        payload = {"error": {"message": response.text[:300]}}
    return response.status_code, payload


def extract_invalid_metric(payload: dict) -> str | None:
    message = json.dumps(payload, ensure_ascii=False)
    marker = "metric "
    lower_message = message.lower()
    if marker not in lower_message:
        return None

    start = lower_message.find(marker)
    snippet = message[start : start + 200]
    quote_chars = ["'", '"']
    for quote in quote_chars:
        if quote in snippet:
            parts = snippet.split(quote)
            if len(parts) >= 3:
                return parts[1]
    return None


def flatten_insights(payload: dict) -> dict[str, object]:
    out: dict[str, object] = {}
    for item in payload.get("data", []):
        name = item.get("name")
        values = item.get("values", [])
        out[name] = values[0].get("value") if values else None
    return out


def get_insights_safe(config: Config, media_item: dict) -> dict[str, object]:
    media_type = media_item.get("media_type")
    metrics = REELS_METRICS.copy() if media_type == "VIDEO" else FEED_METRICS.copy()
    dropped: list[str] = []

    while metrics:
        status, payload = insights_request(config, media_item["id"], metrics)
        if status == 200:
            return {
                "status": status,
                "metrics_used": metrics,
                "metrics_dropped": dropped,
                "metrics": flatten_insights(payload),
            }

        invalid_metric = extract_invalid_metric(payload)
        if invalid_metric and invalid_metric in metrics:
            metrics.remove(invalid_metric)
            dropped.append(invalid_metric)
            continue

        return {
            "status": status,
            "metrics_used": metrics,
            "metrics_dropped": dropped,
            "metrics": {},
        }

    return {
        "status": 400,
        "metrics_used": [],
        "metrics_dropped": dropped,
        "metrics": {},
    }


def build_media_insights(
    config: Config, *, page_limit: int, max_pages: int
) -> list[dict[str, object]]:
    items = get_media_list_all(
        config,
        page_limit=page_limit,
        max_pages=max_pages,
    )
    rows: list[dict[str, object]] = []

    for item in items:
        result = get_insights_safe(config, item)
        row = {
            "media_id": item.get("id"),
            "media_type": item.get("media_type"),
            "is_reel": item.get("media_type") == "VIDEO",
            "permalink": item.get("permalink"),
            "timestamp": item.get("timestamp"),
            "caption": item.get("caption", ""),
            "status": result["status"],
            "metrics_used": ",".join(result["metrics_used"]),
            "metrics_dropped": ",".join(result["metrics_dropped"]),
        }
        for metric in MEDIA_OUTPUT_COLUMNS[9:]:
            row[metric] = result["metrics"].get(metric)
        rows.append(row)
        time.sleep(0.25)

    return rows


def write_csv(
    path: Path,
    rows: Iterable[dict[str, object]],
    fieldnames: list[str],
    *,
    delimiter: str = ",",
    quoting: int = csv.QUOTE_MINIMAL,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            delimiter=delimiter,
            quoting=quoting,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        sample = handle.read(2048)
        handle.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        reader = csv.DictReader(handle, dialect=dialect)
        return list(reader)


def build_demographics_raw() -> list[dict[str, object]]:
    dimension_map = {"age": "edad", "gender": "género"}
    rows: list[dict[str, object]] = []

    for breakdown in ["gender", "age"]:
        file_rows = read_csv(PROCESSED_DIR / f"ig_follower_demographics_{breakdown}.csv")
        for row in file_rows:
            rows.append(
                {
                    "dimension": dimension_map[row["breakdown"]],
                    "category": row["category"],
                    "total_followers": row["value"],
                }
            )

    return rows


def build_geographics_raw() -> list[dict[str, object]]:
    dimension_map = {"city": "ciudad", "country": "país"}
    base_rows: list[dict[str, object]] = []
    region_totals: dict[str, int] = {}

    for breakdown in ["city", "country"]:
        file_rows = read_csv(PROCESSED_DIR / f"ig_follower_demographics_{breakdown}.csv")
        for row in file_rows:
            category = row["category"]
            if breakdown == "city" and "," in category:
                city, region = [part.strip() for part in category.split(",", 1)]
                category = city
                region_totals[region] = region_totals.get(region, 0) + int(row["value"])

            base_rows.append(
                {
                    "dimension": dimension_map[row["breakdown"]],
                    "category": category,
                    "total_followers": row["value"],
                }
            )

    for region, total_followers in sorted(region_totals.items()):
        base_rows.append(
            {
                "dimension": "region",
                "category": region,
                "total_followers": total_followers,
            }
        )

    return base_rows


def transform_for_tableau() -> None:
    totals_since_rows = read_csv(PROCESSED_DIR / "ig_totals_since_creation.csv")
    totals_window_rows = read_csv(PROCESSED_DIR / "ig_totals_by_window_long.csv")
    demographics_rows = build_demographics_raw()
    geographics_rows = build_geographics_raw()

    write_csv(
        RAW_DIR / "total_metrics.csv",
        totals_since_rows,
        ["metric", "value"],
        delimiter=";",
        quoting=csv.QUOTE_ALL,
    )
    write_csv(
        RAW_DIR / "total_metrics_by_window.csv",
        totals_window_rows,
        ["window_start", "window_end", "metric", "value"],
        delimiter=";",
        quoting=csv.QUOTE_ALL,
    )
    write_csv(
        RAW_DIR / "demographics_IG.csv",
        demographics_rows,
        ["dimension", "category", "total_followers"],
        delimiter=";",
        quoting=csv.QUOTE_ALL,
    )
    write_csv(
        RAW_DIR / "geographics_IG.csv",
        geographics_rows,
        ["dimension", "category", "total_followers"],
        delimiter=";",
        quoting=csv.QUOTE_ALL,
    )


def extract_pipeline(config: Config, *, skip_media: bool, page_limit: int, max_pages: int) -> None:
    totals_since_rows = totals_since_creation(config)
    totals_window_rows = totals_by_window(config)

    write_csv(
        PROCESSED_DIR / "ig_totals_since_creation.csv",
        totals_since_rows,
        ["metric", "value"],
    )
    write_csv(
        PROCESSED_DIR / "ig_totals_by_window_long.csv",
        totals_window_rows,
        ["window_start", "window_end", "metric", "value"],
    )

    for breakdown in ["age", "gender", "country", "city"]:
        demo_rows = follower_demographics(config, breakdown)
        write_csv(
            PROCESSED_DIR / f"ig_follower_demographics_{breakdown}.csv",
            demo_rows,
            ["breakdown", "category", "value"],
        )

    if not skip_media:
        media_rows = build_media_insights(
            config,
            page_limit=page_limit,
            max_pages=max_pages,
        )
        write_csv(
            RAW_DIR / "media_insights.csv",
            media_rows,
            MEDIA_OUTPUT_COLUMNS,
            delimiter=";",
            quoting=csv.QUOTE_ALL,
        )


def main() -> int:
    args = parse_args()
    ensure_dirs()
    try:
        if args.command in {"extract", "all"}:
            config = load_config()
            extract_pipeline(
                config,
                skip_media=args.skip_media,
                page_limit=args.page_limit,
                max_pages=args.max_pages,
            )

        if args.command in {"transform", "all"}:
            transform_for_tableau()

        print(f"Proceso completado: {args.command}")
        return 0
    except InstagramPipelineError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
