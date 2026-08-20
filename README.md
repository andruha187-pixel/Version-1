# Strategy Simulator V3 — Binance Regime Research

Отдельный out-of-sample тест. V1/V2 менять не нужно.

## Что торгуется
Все 12 исходных стратегий работают на Polymarket как BASE и не изменены.
Binance BTCUSDT Futures используется только как внешний фильтр.

## Контроль
CONF60 — тот же порог confidence >= 60 из V2.

## Новые V3 shadow-фильтры
- V3_REGIME:
  CONF60 + regime != MIXED

- V3_DIR100:
  CONF60 + direction_changes >= 100

- V3_REGIME_OR_DIR100:
  CONF60 + (regime != MIXED OR direction_changes >= 100)
  Это главный кандидат для проверки на новых данных.

- V3_PATH025:
  CONF60 + path_efficiency <= 0.25

- V3_EXHAUST_GUARD:
  CONF60, но блокирует потенциально поздний/выдохшийся импульс,
  если BTC уже заметно ушёл от цены старта 5m и за последнюю 1 секунду
  всё ещё резко идёт в ту же сторону.

## Binance-признаки
V3 сохраняет все признаки V2:
250ms/500ms/1s/3s/10s returns, flow 1/3/10/30s,
book imbalance, large trades, EMA9/21, RSI14,
distance from 5m start, path efficiency, direction changes,
TREND/MIXED/CHOP и confidence.

## ZIP каждый час
- обычные BASE-файлы
- binance_v3_features.csv
- binance_shadow_trades.csv
- binance_shadow_results.csv
- binance_shadow_summary.csv
- report.txt

В report.txt есть TOP-20 комбинаций и V3 MODE ROLLUP.

ВАЖНО: не менять пороги во время серии, иначе out-of-sample тест теряет смысл.
