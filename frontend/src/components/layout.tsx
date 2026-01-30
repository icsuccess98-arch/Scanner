'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

export function Header() {
  return (
    <header className="sticky top-0 z-[100] bg-gradient-to-b from-[#16131f] to-[#0a0a12] border-b border-[#2d2640] py-4 px-4 backdrop-blur-md">
      <Link href="/" className="flex items-center justify-center w-full group no-underline">
        <span className="text-[clamp(1.75rem,8vw,2.5rem)] bg-gradient-to-br from-[#a855f7] via-[#8b5cf6] to-[#7c3aed] bg-clip-text text-transparent filter drop-shadow-[0_0_12px_rgba(139,92,246,0.6)] animate-pulse" style={{ fontFamily: "'Pacifico', cursive" }}>
          Seven Thirty
        </span>
      </Link>
    </header>
  )
}

export function MobileNav() {
  const pathname = usePathname()
  
  const navItems = [
    { href: '/', label: 'Home', icon: 'bi-house-door-fill' },
    { href: '/spreads', label: 'Spreads', icon: 'bi-list-stars' },
    { href: '/bankroll', label: 'Bankroll', icon: 'bi-safe2-fill' },
    { href: '/history', label: 'History', icon: 'bi-clock-history' },
  ]

  return (
    <nav className="fixed bottom-0 left-0 right-0 bg-gradient-to-b from-[#12101a] to-[#0d0b12] border-t border-[#2d2640] z-[1000] pb-[env(safe-area-inset-bottom,0.5rem)] pt-2 px-2">
      <ul className="flex justify-around items-center list-none m-0 p-0 max-w-md mx-auto">
        {navItems.map(item => {
          const active = pathname === item.href
          return (
            <li key={item.href} className="flex-1">
              <Link href={item.href} className={`flex flex-col items-center p-2 text-[10px] font-bold transition-all no-underline ${active ? 'text-[#FFD700]' : 'text-[#9990B0]'}`}>
                <i className={`bi ${item.icon} text-xl mb-0.5 ${active ? 'drop-shadow-[0_0_6px_rgba(255,215,0,0.5)] scale-110' : ''}`}></i>
                {item.label}
              </Link>
            </li>
          )
        })}
      </ul>
    </nav>
  )
}
