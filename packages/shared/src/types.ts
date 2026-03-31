/** Supported ad platforms. */
export type Platform = 'google_ads' | 'meta';

/** A single client account. */
export interface Client {
  id: string;
  name: string;
  accounts: AdAccount[];
}

/** A platform-specific ad account tied to a client. */
export interface AdAccount {
  id: string;
  platform: Platform;
  externalId: string; /** The account ID on the platform */
  label?: string;
}

/** Aggregated campaign performance row (platform-normalised). */
export interface CampaignMetrics {
  date: string;         // ISO 8601
  platform: Platform;
  accountId: string;
  campaignId: string;
  campaignName: string;
  impressions: number;
  clicks: number;
  spend: number;        // USD
  conversions: number;
  revenue: number;      // USD
}

/** Budget pacing snapshot for a campaign. */
export interface BudgetPacing {
  platform: Platform;
  accountId: string;
  campaignId: string;
  campaignName: string;
  dailyBudget: number;
  monthlyBudget: number | null;
  spentToday: number;
  spentThisMonth: number;
  pacingPercent: number; // 0–100 where 100 = on pace
}
