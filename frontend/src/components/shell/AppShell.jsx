import Sidebar from './Sidebar.jsx';
import TopBar from './TopBar.jsx';

// App shell

export default function AppShell({ title, subtitle, actions, children }) {
  return (
    <div className="flex min-h-screen bg-slate-50">
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0">
        <TopBar title={title} subtitle={subtitle} actions={actions} />
        <main className="flex-1 px-6 py-6 overflow-auto">{children}</main>
      </div>
    </div>
  );
}
