import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ClipboardList, ChevronDown, ChevronUp } from 'lucide-react';
import { getCustomerHistory } from '../../api.js';
import Card from '../ui/Card.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import { formatCurrency, formatNumber, formatDate } from '../../lib/format.js';

// Order history view

// Renders the customer's purchase history. The API returns flat line
// items grouped by purchase_id; we group them client-side and show
// expandable rows.

const PAGE_SIZE = 50;

export default function OrderHistoryView({ custId, mineMode = 0 }) {
  // mineMode is reserved for future use when /customers/me/history exists.
  // For now both customer-self and seller-viewing use /customers/{id}/history.
  const [page, setPage] = useState(0);
  const offset = page * PAGE_SIZE;

  const { data, isLoading, isError } = useQuery({
    queryKey: ['customer', custId, 'history', PAGE_SIZE, offset],
    queryFn: () => getCustomerHistory(custId, { limit: PAGE_SIZE, offset }),
    enabled: Boolean(custId)
  });

  const grouped = useMemo(() => {
    const items = (data && data.items) || [];
    const map = new Map();
    items.forEach((row) => {
      const key = row.purchase_id;
      if (!map.has(key)) {
        map.set(key, {
          purchase_id: key,
          sold_at: row.sold_at,
          lines: [],
          total: 0,
          total_units: 0
        });
      }
      const order = map.get(key);
      order.lines.push(row);
      order.total += parseFloat(row.unit_price || 0) * (row.quantity || 0);
      order.total_units += row.quantity || 0;
    });
    // Sort orders by date desc
    return Array.from(map.values()).sort((a, b) => new Date(b.sold_at) - new Date(a.sold_at));
  }, [data]);

  if (isLoading) return <FullPanelSpinner label="Loading order history" />;
  if (isError) return <EmptyState title="Could not load order history" description="Please try again." />;

  const totalLines = (data && data.total_lines) || 0;
  const returned = (data && data.returned) || 0;

  if (grouped.length === 0) {
    return (
      <Card>
        <EmptyState
          icon={ClipboardList}
          title="No purchase history"
          description="No completed purchases on record yet."
        />
      </Card>
    );
  }

  const hasMore = returned === PAGE_SIZE;
  const hasPrev = page > 0;

  return (
    <Card padding="none">
      <div className="px-5 py-3 border-b border-slate-100 flex items-center justify-between flex-wrap gap-2">
        <div className="text-xs text-slate-500">
          {grouped.length} {grouped.length === 1 ? 'order' : 'orders'} ({returned} of {totalLines} lines)
        </div>
      </div>

      <div className="divide-y divide-slate-100">
        {grouped.map((order) => (
          <OrderRow key={order.purchase_id} order={order} />
        ))}
      </div>

      {(hasPrev || hasMore) ? (
        <div className="border-t border-slate-100 px-5 py-3 flex items-center justify-between">
          <button
            type="button"
            onClick={() => setPage(Math.max(0, page - 1))}
            disabled={!hasPrev}
            className="text-xs font-medium text-slate-600 hover:text-mck-navy disabled:text-slate-300 disabled:cursor-not-allowed px-3 py-1.5 rounded hover:bg-slate-50"
          >
            Previous
          </button>
          <span className="text-xs text-slate-500">Page {page + 1}</span>
          <button
            type="button"
            onClick={() => setPage(page + 1)}
            disabled={!hasMore}
            className="text-xs font-medium text-slate-600 hover:text-mck-navy disabled:text-slate-300 disabled:cursor-not-allowed px-3 py-1.5 rounded hover:bg-slate-50"
          >
            Next
          </button>
        </div>
      ) : null}
    </Card>
  );
}

function OrderRow({ order }) {
  const [expanded, setExpanded] = useState(0);
  return (
    <div>
      <button
        type="button"
        onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
        className="w-full text-left px-5 py-3 hover:bg-slate-50/50 flex items-center gap-4"
      >
        <div className="flex-shrink-0 w-9 h-9 rounded-md bg-mck-sky text-mck-blue flex items-center justify-center">
          <ClipboardList size={16} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="text-sm font-semibold text-mck-navy">Order #{order.purchase_id}</div>
          <div className="text-xs text-slate-500 mt-0.5">
            {formatDate(order.sold_at)} &middot; {order.lines.length} {order.lines.length === 1 ? 'line' : 'lines'} &middot; {formatNumber(order.total_units)} units
          </div>
        </div>
        <div className="text-right">
          <div className="text-sm font-bold text-mck-navy">{formatCurrency(order.total)}</div>
        </div>
        {expanded === 1 ? <ChevronUp size={16} className="text-slate-400" /> : <ChevronDown size={16} className="text-slate-400" />}
      </button>

      {expanded === 1 ? (
        <div className="px-5 pb-3 bg-slate-50/40">
          <div className="bg-white border border-slate-200 rounded-md overflow-hidden">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold bg-slate-50">
                  <th className="text-left px-3 py-2">Item</th>
                  <th className="text-left px-3 py-2">Family</th>
                  <th className="text-right px-3 py-2">Qty</th>
                  <th className="text-right px-3 py-2">Unit price</th>
                  <th className="text-right px-3 py-2">Line total</th>
                </tr>
              </thead>
              <tbody>
                {order.lines.map((line) => {
                  const lineTotal = parseFloat(line.unit_price || 0) * (line.quantity || 0);
                  return (
                    <tr key={`${order.purchase_id}-${line.item_id}`} className="border-t border-slate-100">
                      <td className="px-3 py-2 text-mck-navy font-medium">
                        <div className="truncate max-w-[20rem]">{line.description || `Item ${line.item_id}`}</div>
                        <div className="text-[10px] text-slate-400">SKU {line.item_id}</div>
                      </td>
                      <td className="px-3 py-2 text-slate-600">{line.family || '-'}</td>
                      <td className="px-3 py-2 text-right text-slate-600">{formatNumber(line.quantity)}</td>
                      <td className="px-3 py-2 text-right text-slate-600">{formatCurrency(line.unit_price)}</td>
                      <td className="px-3 py-2 text-right text-mck-navy font-semibold">{formatCurrency(lineTotal)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </div>
  );
}
