import { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { LogOut, User as UserIcon, ChevronDown, Lock } from 'lucide-react';
import { useAuth } from '../../auth.jsx';

// Top bar

export default function TopBar({ title, subtitle, actions }) {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [open, setOpen] = useState(0);
  const menuRef = useRef(null);

  useEffect(() => {
    function onDocClick(e) {
      if (menuRef.current && !menuRef.current.contains(e.target)) {
        setOpen(0);
      }
    }
    document.addEventListener('mousedown', onDocClick);
    return () => document.removeEventListener('mousedown', onDocClick);
  }, []);

  function goChangePassword() {
    setOpen(0);
    navigate('/account/password');
  }

  return (
    <header className="bg-white border-b border-slate-200 px-6 py-3 flex items-center justify-between sticky top-0 z-20">
      <div className="min-w-0">
        {title ? (
          <h1 className="text-base font-semibold text-mck-navy truncate">{title}</h1>
        ) : null}
        {subtitle ? <p className="text-xs text-slate-500 truncate">{subtitle}</p> : null}
      </div>

      <div className="flex items-center gap-3">
        {actions ? <div className="flex items-center gap-2">{actions}</div> : null}

        <div className="relative" ref={menuRef}>
          <button
            type="button"
            onClick={() => setOpen(open === 1 ? 0 : 1)}
            className="flex items-center gap-2 px-3 py-1.5 rounded-md hover:bg-slate-100"
          >
            <div className="w-7 h-7 rounded-full bg-mck-blue text-white flex items-center justify-center text-xs font-semibold">
              {initials(user)}
            </div>
            <div className="text-left hidden md:block">
              <div className="text-xs font-semibold text-mck-navy leading-tight">
                {(user && user.full_name) || (user && user.username) || 'User'}
              </div>
              <div className="text-[10px] uppercase tracking-wider text-slate-500">
                {user && user.role}
              </div>
            </div>
            <ChevronDown size={14} className="text-slate-400" />
          </button>

          {open === 1 ? (
            <div className="absolute right-0 mt-1.5 w-56 bg-white border border-slate-200 rounded-md shadow-lg overflow-hidden z-30">
              <div className="px-3 py-2 border-b border-slate-100">
                <div className="flex items-center gap-2">
                  <UserIcon size={14} className="text-slate-400" />
                  <div className="text-xs font-semibold text-mck-navy truncate">
                    {(user && user.username) || ''}
                  </div>
                </div>
                {user && user.email ? (
                  <div className="text-xs text-slate-500 mt-0.5 truncate">{user.email}</div>
                ) : null}
              </div>
              <button
                type="button"
                onClick={goChangePassword}
                className="w-full flex items-center gap-2 px-3 py-2 text-sm text-slate-700 hover:bg-slate-50"
              >
                <Lock size={14} className="text-slate-500" />
                Change password
              </button>
              <button
                type="button"
                onClick={logout}
                className="w-full flex items-center gap-2 px-3 py-2 text-sm text-red-600 hover:bg-red-50 border-t border-slate-100"
              >
                <LogOut size={14} />
                Sign out
              </button>
            </div>
          ) : null}
        </div>
      </div>
    </header>
  );
}

function initials(user) {
  if (!user) return '?';
  if (user.full_name) {
    const parts = user.full_name.trim().split(/\s+/);
    if (parts.length >= 2) return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    return user.full_name.slice(0, 2).toUpperCase();
  }
  return (user.username || '?').slice(0, 2).toUpperCase();
}
