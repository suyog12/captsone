import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { Trophy, ChevronRight, Building2, AlertCircle } from 'lucide-react';
import Card, { CardHeader } from '../ui/Card.jsx';
import LifecycleBadge from '../ui/LifecycleBadge.jsx';
import SegmentedControl from '../ui/SegmentedControl.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import { getTopCustomers } from '../../api.js';
import { formatCurrency, formatNumber } from '../../lib/format.js';

// Top customers panel

const RANGE_OPTIONS = [
  { value: '30d', label: '30d' },
  { value: '90d', label: '90d' },
  { value: '1y', label: '1y' },
  { value: 'all', label: 'All' }
];

export default function TopCustomersPanel() {
  const navigate = useNavigate();
  const [range, setRange] = useState('all');

  const { data, isLoading, isError } = useQuery({
    queryKey: ['admin', 'top-customers', range, 10],
    queryFn: () => getTopCustomers({ limit: 10, range })
  });

  const rows = (data && data.rows) || [];
  // Compute the total of the visible top-10 to show a relative bar per row
  const maxRevenue = rows.length > 0 ? Number(rows[0].total_revenue) : 1;

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <Trophy size={16} className="text-mck-orange" />
          <div>
            <h3 className="text-sm font-semibold text-mck-navy">Top customers by revenue</h3>
            <p className="text-xs text-slate-500 mt-0.5">Highest-spending accounts in the platform</p>
          </div>
        </div>
        <SegmentedControl
          options={RANGE_OPTIONS}
          value={range}
          onChange={setRange}
          size="sm"
        />
      </div>

      {isLoading ? (
        <FullPanelSpinner label="Loading top customers" />
      ) : isError ? (
        <div className="px-5 pb-5">
          <EmptyState icon={AlertCircle} title="Could not load" description="Please try again." />
        </div>
      ) : rows.length === 0 ? (
        <div className="px-5 pb-5">
          <EmptyState
            icon={Building2}
            title="No revenue data"
            description="No purchases recorded in this range."
          />
        </div>
      ) : (
        <ul className="divide-y divide-slate-100">
          {rows.map((row, idx) => {
            const revenue = Number(row.total_revenue) || 0;
            const barPct = maxRevenue > 0 ? (revenue / maxRevenue) * 100 : 0;
            return (
              <li key={row.cust_id}>
                <button
                  type="button"
                  onClick={() => navigate(`/admin/customers/${row.cust_id}`)}
                  className="w-full px-5 py-3 hover:bg-slate-50 transition-colors text-left flex items-center gap-3 group"
                >
                  {/* Rank */}
                  <div
                    className={`flex-shrink-0 w-7 h-7 rounded font-bold text-xs flex items-center justify-center ${
                      idx === 0
                        ? 'bg-mck-orange text-white'
                        : idx < 3
                        ? 'bg-mck-orange/15 text-mck-orange'
                        : 'bg-slate-100 text-slate-500'
                    }`}
                  >
                    {idx + 1}
                  </div>

                  {/* Body */}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-sm font-semibold text-mck-navy truncate">
                        {row.customer_name || `Customer ${row.cust_id}`}
                      </span>
                      <span className="text-[10px] text-slate-400 font-mono">
                        #{row.cust_id}
                      </span>
                      {row.status ? (
                        <LifecycleBadge status={row.status} size="sm" />
                      ) : null}
                    </div>
                    <div className="text-xs text-slate-500 mt-1 flex items-center gap-2 flex-wrap">
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded bg-slate-100 text-slate-600 font-medium">
                        {row.segment || '-'}
                      </span>
                      {row.specialty_code ? (
                        <span>Specialty {row.specialty_code}</span>
                      ) : null}
                      {row.market_code ? (
                        <>
                          <span className="text-slate-300">|</span>
                          <span>{row.market_code}</span>
                        </>
                      ) : null}
                      <span className="text-slate-300">|</span>
                      <span>{formatNumber(row.total_orders)} orders</span>
                    </div>
                    {/* Revenue bar */}
                    <div className="mt-2 h-1 bg-slate-100 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${
                          idx === 0 ? 'bg-mck-orange' : 'bg-mck-blue'
                        }`}
                        style={{ width: `${barPct}%` }}
                      />
                    </div>
                  </div>

                  {/* Revenue */}
                  <div className="flex-shrink-0 text-right">
                    <div className="text-sm font-bold text-mck-navy">
                      {formatCurrency(revenue)}
                    </div>
                    <div className="text-[10px] text-slate-400 mt-0.5">
                      revenue
                    </div>
                  </div>

                  <ChevronRight
                    size={14}
                    className="text-slate-300 group-hover:text-mck-blue flex-shrink-0 transition-colors"
                  />
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );
}
