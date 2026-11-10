"""Evaluate committed scores against NAB windows and optionally the upstream scorer."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from anomaly_detection.evaluation import evaluate, performance, read_scored


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--windows', default='labels/combined_windows.json')
    parser.add_argument('--output', required=True)
    parser.add_argument('--progress')
    parser.add_argument('--nab-root')
    parser.add_argument('--data-root', default='data')
    parser.add_argument('--threshold', type=float, default=3)
    args = parser.parse_args()
    rows = list(read_scored(args.input))
    run_ids = {r.get('run_id') for r in rows if r.get('record_status') in ('accepted', 'calibration')}
    if len(run_ids) != 1:
        parser.error('Evaluate exactly one nonempty run at a time')
    windows = json.loads(Path(args.windows).read_text())
    result = evaluate(rows, windows)
    if result['duplicate_event_ids']:
        parser.error('Duplicate committed event IDs; resolve these before reporting quality')
    if args.progress:
        result['performance'] = performance(rows, args.progress)
    target = Path(args.output)
    target.mkdir(parents=True, exist_ok=True)
    if args.nab_root:
        from anomaly_detection.nab import export_and_score
        result['nab'] = export_and_score(rows, windows, args.data_root, args.nab_root, target / 'nab', args.threshold)
    (target / 'metrics.json').write_text(json.dumps(result, indent=2, allow_nan=False))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    detectors = ['static', 'rolling']
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for axis, key, title in zip(axes, ['window_recall', 'false_alerts_per_1000'], ['Window recall', 'False alerts / 1,000 records']):
        values = [result['aggregate'][d][key] for d in detectors]
        axis.bar(detectors, [v if v is not None else 0 for v in values])
        for index, value in enumerate(values):
            axis.text(index, value or 0, 'N/A' if value is None else f'{value:.3f}', ha='center', va='bottom')
        axis.set_title(title)
    fig.tight_layout()
    fig.savefig(target / 'quality.png', dpi=160)
    plt.close(fig)
    if args.progress and result['performance']['batches']:
        batches = result['performance']['batches']
        fig, axes = plt.subplots(2, 1, figsize=(10, 7))
        axes[0].plot([b['batch_id'] for b in batches], [b['processed_rows_per_second'] for b in batches], label='Processed rows/s')
        axes[0].legend()
        axes[1].plot([b['batch_id'] for b in batches], [sum(s.get('memoryUsedBytes', 0) for s in b['state_operators']) for b in batches])
        axes[1].set_ylabel('State bytes'); axes[1].set_xlabel('Batch ID')
        fig.tight_layout(); fig.savefig(target / 'streaming.png', dpi=160); plt.close(fig)
    print(json.dumps(result['aggregate'], indent=2))
    print(f'Report: {target}')


if __name__ == '__main__':
    main()
