# M03 V2 GATE64 SAFE — single-strategy PAPER bot

Это следующая тестовая версия после GATE64 X2. Остальные стратегии не возвращались.

## Логика

Бот по-прежнему работает только с BTC 5-minute Up/Down Polymarket.

### 1. Сначала ждём именно V2-eligible сигнал

Сырой M03-сигнал считается кандидатом для первого решения только если одновременно:

```text
price:    0.55–0.75
momentum: 0.03–0.30
lookback: 2 тика
```

Поэтому, например, M03-сигнал по цене `0.50` больше НЕ blacklist'ит рынок. Он просто игнорируется.

### 2. Первый V2-eligible сигнал решает рынок

Когда впервые появился кандидат из диапазона выше:

```text
SAFE PASS:
price    0.64–0.75
momentum 0.05–0.10
```

Если первый V2-eligible сигнал не проходит SAFE-фильтр — рынок пропускается навсегда.

Причины пишутся в `gate_decisions.csv`:

```text
SAFE_PRICE_LOW
SAFE_PRICE_HIGH
SAFE_MOMENTUM_LOW
SAFE_MOMENTUM_HIGH
SAFE_ENTRY_OK
```

### 3. Размеры позиции

```text
ENTRY    = 5 shares
PYRAMID  = 10 shares
```

PYRAMID разрешён только на той же стороне после роста цены ещё на:

```text
+0.08
```

У PYRAMID сохраняется старая V2-логика momentum:

```text
momentum > 0
momentum <= 0.30
```

Максимум:

```text
2 покупки
15 shares всего
```

SWITCH отключён.

## position_trajectory.csv

В каждый часовой ZIP добавлен новый файл `position_trajectory.csv`.

После открытия позиции примерно каждые 3 секунды записываются:

- elapsed_sec;
- primary_best_bid / primary_best_ask;
- opposite_best_bid / opposite_best_ask;
- position_shares;
- gross_entry_cost;
- entry_fees;
- total_cost;
- сколько shares реально можно было бы продать по текущему bid-depth;
- exit_avg_price;
- комиссия условного taker-выхода;
- exit_net_proceeds;
- `unrealized_pnl`;
- `mfe_pnl` — лучший executable PnL после входа;
- `mae_pnl` — худший executable PnL после входа.

Это ничего не закрывает. Файл нужен, чтобы после накопления новых сделок подобрать стоп по реальным траекториям победителей и проигравших.

Если видимой bid-глубины не хватает для выхода всей позиции, `unrealized_pnl` не считается как полноценный executable PnL.

## Telegram

Сохранены START / STOP / BALANCE / STATISTICS / POSITIONS / TRADES / PAPER / LIVE / EMERGENCY STOP.

После нового запуска торговля по умолчанию OFF — нажми START.

Версия остаётся PAPER-only. LIVE заблокирован.

## Часовой ZIP

Каждый завершённый UTC-час бот по-прежнему присылает ZIP в Telegram примерно через 5 минут.

Внутри:

```text
strategy_summary.csv
variants_summary.csv
gate_decisions.csv
paper_trades.csv
signals.csv
market_results.csv
markets.csv
position_trajectory.csv
report.txt
```

`variants_summary.csv` оставлен для совместимости с нашим старым анализом архивов.

## Новая база

```text
/var/data/gate64_safe_trading_bot.db
```

## Render

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
python main.py
```

Persistent disk:

```text
/var/data
```

Существующие `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID` оставь.
Новые переменные можно не добавлять — значения уже встроены по умолчанию.

## Проверка

```text
python test_gate64_safe.py
```

Ожидаемый результат:

```text
GATE64 SAFE regression: OK
```

Параметры лучше пока не менять, чтобы следующие отчёты были настоящим out-of-sample тестом.
