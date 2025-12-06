"""
Cron Jobs - автоматические задачи для управления сайтами
- Предупреждения об истечении хостинга
- Автоматическое отключение сайтов
- Автоматическое удаление сайтов
"""
import asyncio
import logging
from datetime import datetime, timezone
import httpx

import db
from config import settings

log = logging.getLogger(__name__)


async def send_telegram_notification(tg_id: int, message: str):
    """Send notification to manager via bot."""
    if not settings.BOT_WEBHOOK_URL:
        log.debug("BOT_WEBHOOK_URL not configured, skipping notification")
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{settings.BOT_WEBHOOK_URL}/webhook",
                json={
                    "action": "notification",
                    "tg_id": tg_id,
                    "message": message
                },
                timeout=5.0
            )
            log.info(f"Sent notification to manager {tg_id}")
    except Exception as e:
        log.error(f"Failed to send notification to {tg_id}: {e}")


async def check_payment_warnings():
    """Check and send payment warnings (2 weeks before expiry)."""
    log.info("Checking sites needing payment warnings...")

    sites = await db.get_sites_needing_payment_warning()

    for site in sites:
        days_left = (site['hosting_expires_at'] - datetime.now(timezone.utc)).days

        message = (
            f"⚠️ У вашего сайта «{site['company_name']}» скоро истечет срок хостинга!\n\n"
            f"Осталось дней: {days_left}\n"
            f"Дата истечения: {site['hosting_expires_at'].strftime('%d.%m.%Y')}\n\n"
            f"Продлите хостинг, чтобы избежать отключения сайта."
        )

        await send_telegram_notification(site['manager_tg_id'], message)
        await db.mark_payment_warning_sent(site['id'])

        log.info(f"Sent payment warning for site {site['id']}, {days_left} days left")


async def auto_disable_expired_sites():
    """Auto-disable sites that expired 2 weeks ago."""
    log.info("Checking sites to auto-disable...")

    sites = await db.get_sites_to_auto_disable()

    for site in sites:
        # Stop site in deploy-node if deployed
        if site.get('deploy_id') and settings.DEPLOY_NODE_URL:
            try:
                async with httpx.AsyncClient() as client:
                    # TODO: Call deploy-node API to stop the site
                    log.info(f"Would stop site {site['deploy_id']} in deploy-node")
            except Exception as e:
                log.error(f"Failed to stop site in deploy-node: {e}")

        # Mark as stopped in DB
        await db.mark_site_auto_disabled(site['id'])

        # Send notification
        message = (
            f"🔴 Сайт «{site['company_name']}» был отключен из-за неоплаты хостинга.\n\n"
            f"Срок хостинга истек 2 недели назад.\n"
            f"Для восстановления работы сайта необходимо продлить хостинг."
        )

        await send_telegram_notification(site['manager_tg_id'], message)
        await db.schedule_site_for_deletion(site['id'])

        log.warning(f"Auto-disabled site {site['id']} due to non-payment")


async def auto_delete_expired_sites():
    """Auto-delete sites that expired 2 months ago."""
    log.info("Checking sites to auto-delete...")

    sites = await db.get_sites_to_delete()

    for site in sites:
        # Delete from deploy-node if deployed
        if site.get('deploy_id') and settings.DEPLOY_NODE_URL:
            try:
                async with httpx.AsyncClient() as client:
                    # TODO: Call deploy-node API to delete the site
                    log.info(f"Would delete site {site['deploy_id']} from deploy-node")
            except Exception as e:
                log.error(f"Failed to delete site from deploy-node: {e}")

        # Delete from DB
        await db.delete_client_site(site['id'])

        # Send notification
        message = (
            f"🗑️ Сайт «{site['company_name']}» был удален из-за длительной неоплаты.\n\n"
            f"Срок хостинга истек более 2 месяцев назад.\n"
            f"Все данные сайта удалены без возможности восстановления."
        )

        await send_telegram_notification(site['manager_tg_id'], message)

        log.warning(f"Auto-deleted site {site['id']} due to long non-payment")


async def run_cron_jobs():
    """Run all cron jobs."""
    try:
        await check_payment_warnings()
        await auto_disable_expired_sites()
        await auto_delete_expired_sites()
        log.info("Cron jobs completed successfully")
    except Exception as e:
        log.error(f"Error running cron jobs: {e}")


async def start_cron_scheduler():
    """Start cron scheduler (runs every hour)."""
    while True:
        try:
            await run_cron_jobs()
        except Exception as e:
            log.error(f"Cron scheduler error: {e}")

        # Wait 1 hour before next run
        await asyncio.sleep(3600)


if __name__ == "__main__":
    # For testing
    import asyncio
    asyncio.run(run_cron_jobs())

