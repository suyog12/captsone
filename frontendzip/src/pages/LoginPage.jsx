import { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Lock, User, Loader2, AlertCircle } from 'lucide-react';
import { useAuth } from '../auth.jsx';

// Login page

const DEMO_USERS = [
  { label: 'Admin', username: 'admin', role: 'admin' },
  { label: 'Seller', username: 'seller', role: 'seller' },
  { label: 'Customer (stable)', username: 'demo_po_enterprise_stable_7626', role: 'customer' },
  { label: 'Customer (declining)', username: 'demo_ltc_enterprise_declining_6753', role: 'customer' },
  { label: 'Customer (churned)', username: 'demo_ltc_large_churned_0500', role: 'customer' },
  { label: 'Customer (cold start)', username: 'demo_po_new_cold_7181', role: 'customer' }
];

function defaultPathForRole(role) {
  if (role === 'admin') return '/admin';
  if (role === 'seller') return '/seller';
  if (role === 'customer') return '/customer';
  return '/';
}

export default function LoginPage() {
  const { login, user } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('Demo1234!');
  const [submitting, setSubmitting] = useState(0);
  const [errorMsg, setErrorMsg] = useState('');

  // Already logged in: bounce to role default (must be in effect, not render body)
  useEffect(() => {
    if (user) {
      const dest =
        (location.state && location.state.from && location.state.from.pathname) ||
        defaultPathForRole(user.role);
      navigate(dest, { replace: true });
    }
  }, [user, navigate, location.state]);

  async function handleSubmit(e) {
    e.preventDefault();
    if (!username || !password) return;
    setErrorMsg('');
    setSubmitting(1);
    try {
      const u = await login(username.trim(), password);
      navigate(defaultPathForRole(u.role), { replace: true });
    } catch (err) {
      // Surface the real reason. If the server returned 200 but something
      // else broke (network, parsing, redirect), say so honestly.
      const status = err && err.response && err.response.status;
      const detail = err && err.response && err.response.data && err.response.data.detail;
      let msg;
      if (typeof detail === 'string' && detail) {
        msg = detail;
      } else if (status === 401) {
        msg = 'Incorrect username or password.';
      } else if (status) {
        msg = `Sign-in failed (HTTP ${status}). See console for details.`;
      } else {
        msg = (err && err.message) || 'Sign-in failed. See console for details.';
      }
      // eslint-disable-next-line no-console
      console.error('Login error:', err);
      setErrorMsg(msg);
    } finally {
      setSubmitting(0);
    }
  }

  function pickDemo(u) {
    setUsername(u.username);
    setPassword('Demo1234!');
    setErrorMsg('');
  }

  return (
    <div className="min-h-screen flex bg-mck-navy">
      {/* Left brand panel */}
      <div className="hidden lg:flex lg:w-1/2 flex-col justify-between p-12 bg-mck-navy-deep relative overflow-hidden">
        <div className="absolute top-0 left-0 w-1 h-32 bg-mck-orange" />
        <div>
          <div className="text-white text-sm font-semibold tracking-widest uppercase">McKesson</div>
          <div className="text-mck-blue-light text-xs mt-1 tracking-wider">Medical-Surgical Solutions</div>
        </div>
        <div className="relative z-10">
          <h1 className="text-white text-4xl font-semibold leading-tight">
            Recommendation
            <br />
            Intelligence Platform
          </h1>
          <p className="text-slate-300 mt-4 text-base max-w-md leading-relaxed">
            Eight signal types. Specialty-aware ranking. Transparent pitch reasoning behind every recommendation.
          </p>
          <div className="mt-10 grid grid-cols-3 gap-4 max-w-md">
            <Stat label="Signal types" value="8" />
            <Stat label="API endpoints" value="42" />
            <Stat label="QC pass rate" value="99.2%" />
          </div>
        </div>
        <div className="text-slate-400 text-xs">Capstone &middot; William &amp; Mary MSBA</div>
      </div>

      {/* Right form panel */}
      <div className="flex-1 flex items-center justify-center p-6 lg:p-12 bg-slate-50">
        <div className="w-full max-w-md">
          <div className="lg:hidden mb-8 text-center">
            <div className="text-mck-navy text-xl font-semibold tracking-widest uppercase">McKesson</div>
            <div className="text-mck-blue text-xs mt-1">Recommendation Intelligence</div>
          </div>

          <div className="bg-white rounded-xl shadow-card p-8">
            <h2 className="text-2xl font-semibold text-mck-navy">Sign in</h2>
            <p className="text-sm text-slate-500 mt-1">Enter your credentials to continue.</p>

            <form onSubmit={handleSubmit} className="mt-6 space-y-4">
              <Field
                icon={<User size={16} />}
                label="Username"
                type="text"
                autoComplete="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="your.username"
              />
              <Field
                icon={<Lock size={16} />}
                label="Password"
                type="password"
                autoComplete="current-password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Password"
              />

              {errorMsg ? (
                <div className="flex items-start gap-2 bg-red-50 border border-red-200 rounded-md px-3 py-2 text-sm text-red-700">
                  <AlertCircle size={16} className="flex-shrink-0 mt-0.5" />
                  <span>{errorMsg}</span>
                </div>
              ) : null}

              <button
                type="submit"
                disabled={submitting === 1}
                className="w-full flex items-center justify-center gap-2 bg-mck-blue hover:bg-mck-blue-dark disabled:bg-slate-300 disabled:cursor-not-allowed text-white font-medium py-2.5 rounded-md transition-colors"
              >
                {submitting === 1 ? (
                  <>
                    <Loader2 size={16} className="animate-spin" />
                    Signing in...
                  </>
                ) : (
                  'Sign in'
                )}
              </button>
            </form>
          </div>

          {/* Demo shortcuts */}
          <div className="mt-6 bg-white rounded-xl shadow-card p-5">
            <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">
              Demo accounts (password Demo1234!)
            </div>
            <div className="grid grid-cols-2 gap-2">
              {DEMO_USERS.map((u) => (
                <button
                  key={u.username}
                  type="button"
                  onClick={() => pickDemo(u)}
                  className="text-left px-3 py-2 rounded border border-slate-200 hover:border-mck-blue hover:bg-mck-sky transition-colors"
                >
                  <div className="text-xs font-semibold text-mck-navy">{u.label}</div>
                  <div className="text-xs text-slate-500 truncate">{u.username}</div>
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div>
      <div className="text-mck-orange text-2xl font-bold leading-none">{value}</div>
      <div className="text-slate-400 text-xs mt-1 uppercase tracking-wider">{label}</div>
    </div>
  );
}

function Field({ icon, label, ...rest }) {
  return (
    <label className="block">
      <span className="text-xs font-semibold text-slate-600 uppercase tracking-wider">{label}</span>
      <div className="mt-1 relative">
        <div className="absolute inset-y-0 left-3 flex items-center text-slate-400 pointer-events-none">{icon}</div>
        <input
          {...rest}
          className="w-full pl-9 pr-3 py-2.5 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue placeholder:text-slate-400"
        />
      </div>
    </label>
  );
}
