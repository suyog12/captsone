import { useMemo } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend
} from 'chart.js';
import { Bar } from 'react-chartjs-2';

// Horizontal bar chart

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

const MCK_BLUE = '#1B6BBE';
const MCK_NAVY = '#0D2347';

export default function HorizontalBarChart({
  bars = [],
  valueLabel = 'Value',
  formatValue,
  height = 320,
  color = MCK_BLUE,
  // bars is an array of { label, value, secondary? } where secondary is an
  // optional smaller string shown in the tooltip body (e.g. "12 of 47 conv")
  perRowColors
}) {
  const data = useMemo(
    () => ({
      labels: bars.map((b) => b.label),
      datasets: [
        {
          label: valueLabel,
          data: bars.map((b) => b.value),
          backgroundColor: bars.map((b, i) =>
            (perRowColors && perRowColors[i]) ? perRowColors[i] : color
          ),
          borderColor: 'transparent',
          borderWidth: 0,
          borderRadius: 4,
          barThickness: 20,
          hoverBackgroundColor: bars.map((b, i) =>
            (perRowColors && perRowColors[i]) ? darken(perRowColors[i]) : darken(color)
          )
        }
      ]
    }),
    [bars, valueLabel, color, perRowColors]
  );

  const options = useMemo(
    () => ({
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'nearest', intersect: false },
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
              const raw = ctx.parsed.x;
              const formatted = typeof formatValue === 'function' ? formatValue(raw) : raw;
              const lines = [`${valueLabel}: ${formatted}`];
              const secondary = bars[ctx.dataIndex] && bars[ctx.dataIndex].secondary;
              if (secondary) lines.push(secondary);
              return lines;
            }
          }
        }
      },
      scales: {
        x: {
          beginAtZero: true,
          grid: { color: '#E2E8F0', drawBorder: false },
          ticks: {
            color: '#64748B',
            font: { family: 'Source Sans 3', size: 11 },
            callback: (val) => (typeof formatValue === 'function' ? formatValue(val) : val)
          }
        },
        y: {
          grid: { display: false, drawBorder: false },
          ticks: {
            color: '#0D2347',
            font: { family: 'Source Sans 3', size: 12, weight: 500 }
          }
        }
      }
    }),
    [valueLabel, formatValue, bars]
  );

  if (!bars || bars.length === 0) {
    return (
      <div className="flex items-center justify-center text-sm text-slate-400" style={{ height }}>
        No data to display
      </div>
    );
  }

  return (
    <div style={{ height }}>
      <Bar data={data} options={options} />
    </div>
  );
}

function darken(hex) {
  if (!hex || !hex.startsWith('#') || hex.length !== 7) return hex;
  const r = Math.max(0, parseInt(hex.slice(1, 3), 16) - 30);
  const g = Math.max(0, parseInt(hex.slice(3, 5), 16) - 30);
  const b = Math.max(0, parseInt(hex.slice(5, 7), 16) - 30);
  return `rgb(${r}, ${g}, ${b})`;
}
