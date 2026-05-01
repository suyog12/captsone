import { NavLink } from 'react-router-dom';
import {
  LayoutDashboard,
  Users,
  UserCog,
  ShoppingCart,
  TrendingUp,
  Sparkles,
  ClipboardList,
  Building2,
  Briefcase,
  Activity,
  Package
} from 'lucide-react';
import { useAuth } from '../../auth.jsx';
// Sidebar
const NAV_BY_ROLE = {
  admin: [
    { to: '/admin', label: 'Overview', icon: LayoutDashboard, end: 1 },
    { to: '/admin/recommendations', label: 'Recommendations', icon: Activity },
    { to: '/admin/products', label: 'Products', icon: Package },
    { to: '/admin/catalog', label: 'Catalog', icon: ShoppingCart },
    { to: '/admin/sellers', label: 'Sellers', icon: Briefcase },
    { to: '/admin/customers', label: 'Customers', icon: Building2 },
    { to: '/admin/users', label: 'User management', icon: UserCog }
  ],
  seller: [
    { to: '/seller', label: 'My customers', icon: Users, end: 1 },
    { to: '/seller/performance', label: 'Performance', icon: TrendingUp }
  ],
  customer: [
    { to: '/customer', label: 'Overview', icon: LayoutDashboard, end: 1 },
    { to: '/customer/recommendations', label: 'Recommended', icon: Sparkles },
    { to: '/customer/catalog', label: 'Browse products', icon: Package },
    { to: '/customer/cart', label: 'My cart', icon: ShoppingCart },
    { to: '/customer/orders', label: 'Order history', icon: ClipboardList }
  ]
};
export default function Sidebar() {
  const { user } = useAuth();
  const items = (user && NAV_BY_ROLE[user.role]) || [];
  return (
    <aside className="hidden md:flex md:w-60 lg:w-64 flex-col bg-mck-navy-deep text-white sticky top-0 h-screen overflow-y-auto self-start flex-shrink-0">
      <div className="px-5 py-5 border-b border-white/5 relative flex-shrink-0">
        <div className="absolute top-5 left-0 w-1 h-8 bg-mck-orange rounded-r-sm" />
        <div className="pl-2">
          <div className="text-white text-sm font-semibold tracking-widest uppercase">McKesson</div>
          <div className="text-mck-blue-light text-[10px] mt-0.5 tracking-wider uppercase">
            Recommendation Intelligence
          </div>
        </div>
      </div>
      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {items.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end === 1}
            className={({ isActive }) =>
              `relative flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
                isActive
                  ? 'bg-mck-blue/15 text-white'
                  : 'text-slate-300 hover:bg-white/5 hover:text-white'
              }`
            }
          >
            {({ isActive }) => (
              <>
                {isActive ? (
                  <span className="absolute left-0 top-2 bottom-2 w-0.5 bg-mck-orange rounded-r-sm" />
                ) : null}
                <item.icon size={16} strokeWidth={1.75} className="flex-shrink-0" />
                <span className="font-medium">{item.label}</span>
              </>
            )}
          </NavLink>
        ))}
      </nav>
      <div className="px-5 py-4 border-t border-white/5 text-[10px] text-slate-500 tracking-wide flex-shrink-0">
        Capstone &middot; W&amp;M MSBA
      </div>
    </aside>
  );
}