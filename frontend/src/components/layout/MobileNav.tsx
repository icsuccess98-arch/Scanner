'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

const navItems = [
  { href: '/', icon: 'bi-house-fill', label: 'Home' },
  { href: '/spreads', icon: 'bi-bar-chart-fill', label: 'Spreads' },
  { href: '/bankroll', icon: 'bi-wallet2', label: 'Bankroll' },
  { href: '/history', icon: 'bi-clock-history', label: 'History' },
]

export function MobileNav() {
  const pathname = usePathname()

  return (
    <nav className="mobile-bottom-nav">
      <ul>
        {navItems.map(item => (
          <li key={item.href}>
            <Link href={item.href} className={pathname === item.href ? 'active' : ''}>
              <i className={`bi ${item.icon}`} />
              {item.label}
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  )
}
