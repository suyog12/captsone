import { useQuery } from '@tanstack/react-query';
import { Activity, ShoppingBag, Sparkles, Building2, User } from 'lucide-react';
import { getRecentSales } from '../../api.js';
import Card from '../ui/Card.jsx';
import SignalBadge from '../ui/SignalBadge.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import Expandable from '../ui/Expandable.jsx';
import { formatCurrency, relativeTime } from '../../lib/format.js';

// Recent sales feed

// Polls /admin/stats/recent-sales every 60 seconds. Each row shows the
// item, customer, seller, line total, and a SignalBadge if the sale was
// driven by a recommendation. Use this on admin dashboards.

const POLL_INTERVAL_MS = 60_000;

export default function RecentSalesFeed({
  limit = 25,
  initialVisible = 10,
  title = 'Recent sales (live)',
  subtitle = 'Auto-refreshes every 60 seconds',
  polling = 1
}) {
  const { data, isLoading, isError, isFetching, dataUpdatedAt } = useQuery({
    queryKey: ['admin', 'recent-sales', limit],
    queryFn: () => getRecentSales({ limit }),
    refetchInterval: polling === 1 ? POLL_INTERVAL_MS : false,
    refetchIntervalInBackground: 0
  });

  const rows = (data && data.rows) || [];

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-base font-semibold text-mck-navy">{title}</h2>
            {polling === 1 ? <PulseDot fetching={isFetching ? 1 : 0} /> : null}
          </div>
          <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>
        </div>
        {dataUpdatedAt ? (
          <div className="text-[10px] text-slate-400 font-medium uppercase tracking-wider">
            Updated {relativeTime(new Date(dataUpdatedAt).toISOString())}
          </div>
        ) : null}
      </div>

      <div>
        {isLoading ? (
          <div className="px-5 py-8 text-center text-sm text-slate-400">Loading recent sales...</div>
        ) : isError ? (
          <div className="px-5 py-8">
            <EmptyState title="Could not load recent sales" description="Please try again shortly." />
          </div>
        ) : rows.length === 0 ? (
          <div className="px-5 py-8">
            <EmptyState
              icon={Activity}
              title="No recent sales yet"
              description="Sales activity will appear here as it happens."
            />
          </div>
        ) : (
          <Expandable
            initial={initialVisible}
            divider={1}
            showMoreLabel={`Show all ${rows.length} sales`}
            showLessLabel="Show less"
            className="divide-y divide-slate-100"
          >
            {rows.map((row) => (
              <SaleRow key={row.purchase_id} row={row} />
            ))}
          </Expandable>
        )}
      </div>
    </Card>
  );
}

function SaleRow({ row }) {
  const isRec = row.from_recommendation === true || row.from_recommendation === 1;
  const sourceCode = row.recommendation_source && row.recommendation_source.code;
  const sourceDisplay = (row.recommendation_source && row.recommendation_source.display_name) || sourceCode;

  return (
    <div className="px-5 py-3 hover:bg-slate-50/50">
      <div className="flex items-start gap-3">
        <div
          className={`flex-shrink-0 w-9 h-9 rounded-md flex items-center justify-center ${
            isRec ? 'bg-mck-orange/10 text-mck-orange' : 'bg-slate-100 text-slate-500'
          }`}
        >
          {isRec ? <Sparkles size={16} /> : <ShoppingBag size={16} />}
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-start justify-between gap-3 flex-wrap">
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-sm font-semibold text-mck-navy truncate">
                  {row.item_description || `Item ${row.item_id}`}
                </span>
                {isRec ? <SignalBadge signal={sourceCode} size="xs" /> : null}
              </div>

              <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-x-2 gap-y-0.5 flex-wrap">
                <span className="inline-flex items-center gap-1">
                  <Building2 size={11} className="text-slate-400" />
                  {row.customer_name || `Cust ${row.cust_id}`}
                </span>
                {row.sold_by_seller_username ? (
                  <>
                    <span className="text-slate-300">|</span>
                    <span className="inline-flex items-center gap-1">
                      <User size={11} className="text-slate-400" />
                      {row.sold_by_seller_username}
                    </span>
                  </>
                ) : null}
                {row.family ? (
                  <>
                    <span className="text-slate-300">|</span>
                    <span>{row.family}</span>
                  </>
                ) : null}
                {isRec && sourceDisplay ? (
                  <>
                    <span className="text-slate-300">|</span>
                    <span className="text-mck-orange font-medium">via {sourceDisplay}</span>
                  </>
                ) : null}
              </div>
            </div>

            <div className="text-right flex-shrink-0">
              <div className="text-sm font-bold text-mck-navy">{formatCurrency(row.line_total)}</div>
              <div className="text-[10px] text-slate-400 mt-0.5">
                {row.quantity ? `${row.quantity} x ` : ''}
                {formatCurrency(row.unit_price)}
              </div>
              <div className="text-[10px] text-slate-400 mt-0.5">{relativeTime(row.sold_at)}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function PulseDot({ fetching }) {
  return (
    <span className="relative inline-flex items-center" aria-label={fetching === 1 ? 'Fetching' : 'Live'}>
      <span
        className={`w-1.5 h-1.5 rounded-full bg-green-500 ${fetching === 1 ? 'animate-ping absolute' : ''}`}
      />
      <span className="w-1.5 h-1.5 rounded-full bg-green-500 relative" />
    </span>
  );
}
