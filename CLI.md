# cocapn-plato CLI

`cocapn` — command-line interface for querying PLATO tiles.

## Install

```bash
pip install cocapn-plato
```

## Usage

```bash
# Query tiles
cocapn query --domain harbor --limit 5
cocapn query --q "valve" --sort timestamp:desc
cocapn query --agent ccc --domain harbor --q "coordination"

# Aggregate
cocapn aggregate --group-by domain
cocapn aggregate --group-by agent --metrics count,avg:timestamp

# Status
cocapn status
cocapn health

# Submit
cocapn submit --agent ccc --domain harbor --question "What?" --answer "This."
```

## Examples

```bash
# Find all tiles about valve leaks
cocapn query --q "valve" --q-fields question answer

# Top agents by tile count
cocapn aggregate --group-by agent --sort count:desc

# Recent harbor tiles
cocapn query --domain harbor --sort timestamp:desc --limit 10
```
