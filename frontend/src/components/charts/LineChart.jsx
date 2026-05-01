import { useMemo } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend
} from 'chart.js';
import { Line } from 'react-chartjs-2';

// Line chart

// Register Chart.js elements once. Doing this at module load keeps every
// LineChart instance reusing the same registration.
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip, Legend);

const MCK_BLUE = '#1B6BBE';
const MCK_NAVY = '#0D2347';

export default function LineChart({
  buckets = [],
  valueLabel = 'Value',
  formatValue,
  color = MCK_BLUE,
  height = 280,
  fill = 1
}) {

  // Build the gradient inline as a Chart.js scriptable backgroundColor.
  // Chart.js calls this every draw with the live chartArea, so we never
  // build a gradient against a 0-height canvas (which was the bug).
  const buildBackground = (ctx) => {
    if (fill !== 1) return 'transparent';
    const chart = ctx.chart;
    const { chartArea } = chart;
    if (!chartArea) {
      // Chart not laid out yet — return solid tinted color as a safe fallback.
      // Chart.js will call this again once chartArea is ready.
      return hexWithAlpha(color, 0.15);
    }
    const gradient = chart.ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
    gradient.addColorStop(0, hexWithAlpha(color, 0.25));
    gradient.addColorStop(1, hexWithAlpha(color, 0));
    return gradient;
  };

  const data = useMemo(
    () => ({
      labels: buckets.map((b) => b.label),
      datasets: [
        {
          label: valueLabel,
          data: buckets.map((b) => b.value),
          borderColor: color,
          backgroundColor: buildBackground,
          borderWidth: 2,
          tension: 0.32,
          pointRadius: 0,
          pointHoverRadius: 5,
          pointHoverBackgroundColor: color,
          pointHoverBorderColor: '#FFFFFF',
          pointHoverBorderWidth: 2,
          fill: fill === 1
        }
      ]
    }),
    // buildBackground is a closure over color/fill — re-create dataset when
    // buckets/color/fill change. Including buildBackground would loop because
    // it's a fresh function each render; we don't include it on purpose.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [buckets, valueLabel, color, fill]
  );

  const options = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        // Disable the entry animation that was racing with the gradient
        // build. Tooltips and hover effects still animate normally.
        duration: 0
      },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: MCK_NAVY,
          titleColor: '#FFFFFF',
          bodyColor: '#E2E8F0',
          padding: 10,
          cornerRadius: 6,
          displayColors: false,
          titleFont: { family: 'Source Sans 3', size: 12, weight: 600 },
          bodyFont: { family: 'Source Sans 3', size: 12 },
          callbacks: {
            label: (ctx) => {
              const raw = ctx.parsed.y;
              const formatted = typeof formatValue === 'function' ? formatValue(raw) : raw;
              return `${valueLabel}: ${formatted}`;
            }
          }
        }
      },
      scales: {
        x: {
          grid: { display: false, drawBorder: false },
          ticks: {
            color: '#64748B',
            font: { family: 'Source Sans 3', size: 11 },
            maxRotation: 0,
            autoSkipPadding: 12
          }
        },
        y: {
          beginAtZero: true,
          grid: { color: '#E2E8F0', drawBorder: false },
          ticks: {
            color: '#64748B',
            font: { family: 'Source Sans 3', size: 11 },
            callback: (val) => (typeof formatValue === 'function' ? formatValue(val) : val)
          }
        }
      }
    }),
    [valueLabel, formatValue]
  );

  if (!buckets || buckets.length === 0) {
    return (
      <div className="flex items-center justify-center text-sm text-slate-400" style={{ height }}>
        No data to display
      </div>
    );
  }

  return (
    <div style={{ height }}>
      <Line data={data} options={options} />
    </div>
  );
}

function hexWithAlpha(hex, alpha) {
  // Accept #RRGGBB only (consistent with our brand tokens)
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}