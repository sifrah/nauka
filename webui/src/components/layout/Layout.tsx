import { NavLink, Outlet } from "react-router";
import { FolderOpen, BarChart3 } from "lucide-react";
import * as Tooltip from "@radix-ui/react-tooltip";

const navItems = [
  { to: "/files", label: "Files", icon: FolderOpen },
  { to: "/dashboard", label: "Cluster", icon: BarChart3 },
];

export function Layout() {
  return (
    <Tooltip.Provider delayDuration={300}>
      <div className="flex h-screen">
        <nav className="flex flex-col items-center w-14 border-r border-border bg-card py-3 gap-1 shrink-0">
          <div
            className="w-10 h-10 flex items-center justify-center mb-3 text-lg select-none"
            title="yogfile"
          >
            🧿
          </div>
          {navItems.map(({ to, label, icon: Icon }) => (
            <Tooltip.Root key={to}>
              <Tooltip.Trigger asChild>
                <div>
                  <NavLink
                    to={to}
                    className={({ isActive }) =>
                      isActive
                        ? "flex items-center justify-center w-10 h-10 rounded-lg bg-accent text-foreground border border-border-bright"
                        : "flex items-center justify-center w-10 h-10 rounded-lg text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                    }
                  >
                    <Icon size={18} strokeWidth={1.5} />
                  </NavLink>
                </div>
              </Tooltip.Trigger>
              <Tooltip.Portal>
                <Tooltip.Content
                  side="right"
                  sideOffset={8}
                  className="bg-[#3d444d] text-[#e6edf3] rounded-md px-2.5 py-1 text-xs font-medium z-50 select-none shadow-md animate-[tooltipIn_0.1s_ease-out]"
                >
                  {label}
                </Tooltip.Content>
              </Tooltip.Portal>
            </Tooltip.Root>
          ))}
          <div className="mt-auto pb-1">
            <a
              href="https://github.com/Barre/ZeroFS"
              target="_blank"
              rel="noreferrer"
              className="block text-[9px] leading-tight text-muted-foreground hover:text-foreground text-center px-1"
              title="Interface derived from the ZeroFS webui (AGPL-3.0)"
            >
              UI :<br />
              ZeroFS
            </a>
          </div>
        </nav>
        <main className="flex-1 overflow-auto bg-background relative">
          <Outlet />
        </main>
      </div>
    </Tooltip.Provider>
  );
}
