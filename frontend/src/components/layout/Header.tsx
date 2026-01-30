'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { NAV_LINKS } from '@/lib/constants'

export default function Header() {
  const pathname = usePathname()

  return (
    <header className="sticky top-0 z-50 bg-[var(--card)] border-b border-[var(--border)]">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo */}
          <Link href="/" className="flex items-center gap-2">
            <span className="text-2xl font-bold text-[var(--gold)]">730's</span>
            <span className="text-xl font-semibold text-white">Locks</span>
          </Link>

          {/* Desktop Navigation */}
          <nav className="hidden md:flex items-center gap-1">
            {NAV_LINKS.map((link) => {
              const isActive = pathname === link.href
              return (
                <Link
                  key={link.href}
                  href={link.href}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                    isActive
                      ? 'bg-[var(--purple)] text-white'
                      : 'text-gray-400 hover:text-white hover:bg-[var(--border)]'
                  }`}
                >
                  {link.label}
                </Link>
              )
            })}
          </nav>

          {/* Status indicator */}
          <div className="hidden md:flex items-center gap-2 text-sm text-gray-400">
            <span className="w-2 h-2 rounded-full bg-green-500"></span>
            <span>Live</span>
          </div>
        </div>
      </div>
    </header>
  )
}
