import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Calendar } from 'lucide-react';
import Card from '../ui/Card.jsx';
import { getCustomerHistory } from '../../api.js';
import { formatDate } from '../../lib/format.js';

// Reorder cadence heatmap

// Renders a calendar grid for the last 90 days where each cell's intensity
// reflects the number of order lines on that day. Hovering a cell shows the
// date and order count. Today is on the right; days are arranged in columns
// of weeks (Sunday through Saturday rows), the standard GitHub-style layout.

const DAYS = 90;
const ROW_LABELS = ['', 'Mon', '', 'Wed', '', 'Fri', ''];

export default function ReorderHeatmap({ custId }) {
  // We pull a generous window of recent lines. The history endpoint accepts
  // a limit; 500 is more than enough to cover 90 days for any normal account.
  const { data, isLoading } = useQuery({
    queryKey: ['customer', custId, 'history', 'heatmap-90d'],
    queryFn: () => getCustomerHistory(custId, { limit: 500 }),
    enabled: Boolean(custId)
  });

  const grid = useMemo(() => buildGrid(data), [data]);

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <Calendar size={14} className="text-mck-blue" />
          <h3 className="text-sm font-semibold text-mck-navy">Reorder cadence</h3>
        </div>
        <div className="text-[11px] text-slate-500">Last 90 days</div>
      </div>
      <div className="px-5 pb-5">
        {isLoading ? (
          <div className="h-[120px] flex items-center justify-center text-xs text-slate-400">
            Loading cadence...
          </div>
        ) : grid.totalOrders === 0 ? (
          <div className="h-[120px] flex flex-col items-center justify-center text-xs text-slate-400">
            <Calendar size={20} className="text-slate-300 mb-2" />
            No orders in the last 90 days yet.
          </div>
        ) : (
          <>
            <HeatmapGrid grid={grid} />
            <div className="mt-3 flex items-center justify-between text-[11px] text-slate-500">
              <div>
                <span className="font-semibold text-mck-navy">{grid.activeDays}</span> active days
                {' '}&middot;{' '}
                <span className="font-semibold text-mck-navy">{grid.totalOrders}</span> order lines
              </div>
              <Legend max={grid.maxCount} />
            </div>
          </>
        )}
      </div>
    </Card>
  );
}

function HeatmapGrid({ grid }) {
  return (
    <div className="flex items-start gap-1.5">
      {/* Row labels (day of week) */}
      <div className="flex flex-col gap-[3px] pt-[14px]">
        {ROW_LABELS.map((label, i) => (
          <div key={i} className="h-[12px] text-[9px] text-slate-400 leading-[12px]">
            {label}
          </div>
        ))}
      </div>

      <div className="flex-1 overflow-x-auto">
        {/* Month labels above grid */}
        <div className="flex items-center gap-[3px] mb-[2px] h-[12px]">
          {grid.weeks.map((week, wi) => (
            <div key={wi} className="w-[12px] text-[9px] text-slate-400 leading-[12px]">
              {week.monthLabel || ''}
            </div>
          ))}
        </div>

        {/* Cells */}
        <div className="flex items-start gap-[3px]">
          {grid.weeks.map((week, wi) => (
            <div key={wi} className="flex flex-col gap-[3px]">
              {week.days.map((day, di) => (
                <Cell key={di} day={day} />
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function Cell({ day }) {
  if (!day) {
    return <div className="w-[12px] h-[12px]" />;
  }
  const intensity = day.intensity;
  const bg = intensityClass(intensity);
  const titleParts = [formatDate(day.date)];
  if (day.count > 0) {
    titleParts.push(`${day.count} order line${day.count === 1 ? '' : 's'}`);
  } else {
    titleParts.push('No orders');
  }
  return (
    <div
      title={titleParts.join(' - ')}
      className={`w-[12px] h-[12px] rounded-[2px] ${bg} ${day.isToday ? 'ring-2 ring-mck-blue ring-offset-1' : ''}`}
    />
  );
}

function Legend({ max }) {
  return (
    <div className="flex items-center gap-1">
      <span className="mr-1">Less</span>
      <div className="w-[10px] h-[10px] rounded-[2px] bg-slate-100" />
      <div className="w-[10px] h-[10px] rounded-[2px] bg-mck-blue/20" />
      <div className="w-[10px] h-[10px] rounded-[2px] bg-mck-blue/40" />
      <div className="w-[10px] h-[10px] rounded-[2px] bg-mck-blue/70" />
      <div className="w-[10px] h-[10px] rounded-[2px] bg-mck-blue" />
      <span className="ml-1">More{max > 0 ? ` (${max})` : ''}</span>
    </div>
  );
}

function intensityClass(intensity) {
  if (intensity === 0) return 'bg-slate-100';
  if (intensity === 1) return 'bg-mck-blue/20';
  if (intensity === 2) return 'bg-mck-blue/40';
  if (intensity === 3) return 'bg-mck-blue/70';
  return 'bg-mck-blue';
}

// Build the 90-day grid arranged into weeks of 7 days. Each cell carries
// its date, order count, and a 0-4 intensity bucket scaled against the
// daily max.
function buildGrid(data) {
  const items = (data && data.items) || [];

  // Tally counts per YYYY-MM-DD
  const byDay = new Map();
  for (let i = 0; i < items.length; i = i + 1) {
    const d = items[i].sold_at;
    if (!d) continue;
    const key = ymd(new Date(d));
    byDay.set(key, (byDay.get(key) || 0) + 1);
  }

  // Build the 90-day window ending today
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const todayKey = ymd(today);

  const days = [];
  for (let i = DAYS - 1; i >= 0; i = i - 1) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    const key = ymd(d);
    days.push({
      date: d,
      key,
      count: byDay.get(key) || 0,
      isToday: key === todayKey
    });
  }

  // Find max for intensity scaling, and total stats
  let maxCount = 0;
  let totalOrders = 0;
  let activeDays = 0;
  for (let i = 0; i < days.length; i = i + 1) {
    if (days[i].count > maxCount) maxCount = days[i].count;
    if (days[i].count > 0) {
      activeDays = activeDays + 1;
      totalOrders = totalOrders + days[i].count;
    }
  }

  // Bucket counts to intensity 0-4
  for (let i = 0; i < days.length; i = i + 1) {
    days[i].intensity = bucketIntensity(days[i].count, maxCount);
  }

  // Group into weeks (columns), Sunday=0 row at top
  // We pad the leading week with empty cells so the first column starts on
  // the correct day of week.
  const weeks = [];
  let currentWeek = { days: [], monthLabel: '' };
  // Pad leading slots so the first day lands in its correct weekday row
  const firstDow = days[0].date.getDay();
  for (let i = 0; i < firstDow; i = i + 1) {
    currentWeek.days.push(null);
  }
  let lastMonth = -1;
  for (let i = 0; i < days.length; i = i + 1) {
    const day = days[i];
    if (currentWeek.days.length === 0) {
      // start of a new week column - if month changed, emit a label
      const m = day.date.getMonth();
      if (m !== lastMonth) {
        currentWeek.monthLabel = MONTH_NAMES[m];
        lastMonth = m;
      }
    }
    currentWeek.days.push(day);
    if (currentWeek.days.length === 7) {
      weeks.push(currentWeek);
      currentWeek = { days: [], monthLabel: '' };
    }
  }
  // Pad trailing slots
  if (currentWeek.days.length > 0) {
    while (currentWeek.days.length < 7) {
      currentWeek.days.push(null);
    }
    weeks.push(currentWeek);
  }

  return { weeks, maxCount, totalOrders, activeDays };
}

function bucketIntensity(count, maxCount) {
  if (count === 0) return 0;
  if (maxCount <= 1) return count > 0 ? 4 : 0;
  const ratio = count / maxCount;
  if (ratio <= 0.25) return 1;
  if (ratio <= 0.5) return 2;
  if (ratio <= 0.75) return 3;
  return 4;
}

function ymd(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
