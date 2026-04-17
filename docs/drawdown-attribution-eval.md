# Drawdown Attribution Evaluation

This script is the first lightweight QA layer for drawdown attribution generalization.

## Goal

Instead of reviewing screenshots one-by-one, we track whether attribution quality is improving across:

- cycle families
- business archetypes
- event-signal coverage
- fallback rate
- ordering consistency

## Run

```bash
cd /Users/andersonzhou/Desktop/PB_AIBuddy
npm run attrib:eval
```

Optional custom basket:

```bash
cd /Users/andersonzhou/Desktop/PB_AIBuddy
npm run attrib:eval -- BABA JD BIDU GOOG META TSLA UNH TSM AMD MU COIN HOOD MSTR
```

## What to look for

- **Likely fallback rate**
  - if too high for an archetype, our taxonomy or event extraction is still weak there
- **Signal-backed episodes**
  - tells us whether headlines are being translated into structured event signals
- **Ordering failures**
  - should be zero; otherwise the display pipeline is out of sync
- **Primary driver mix**
  - helps spot systems that are still over-assigning everything to macro

## Interpreting results

- High `company` / `sector` share for cyclical names is generally good
- High `macro` share across everything usually means the model is still too coarse
- Archetypes with high fallback rates should get the next round of taxonomy or rule work
