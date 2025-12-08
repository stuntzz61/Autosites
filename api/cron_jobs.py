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
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(
                        f"{settings.DEPLOY_NODE_URL}/api/sites/by-id/{site['deploy_id']}/stop"
                    )
                    if response.status_code == 200:
                        log.info(f"Stopped site {site['deploy_id']} in deploy-node")
                    elif response.status_code == 404:
                        log.warning(f"Site {site['deploy_id']} not found in deploy-node")
                    else:
                        log.warning(f"Failed to stop site {site['deploy_id']}: {response.status_code}")
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
        # Delete from deploy-node if deployed (stops container, removes files)
        if site.get('deploy_id') and settings.DEPLOY_NODE_URL:
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.delete(
                        f"{settings.DEPLOY_NODE_URL}/api/sites/by-id/{site['deploy_id']}"
                    )
                    if response.status_code == 200:
                        log.info(f"Deleted site {site['deploy_id']} from deploy-node")
                    elif response.status_code == 404:
                        log.warning(f"Site {site['deploy_id']} not found in deploy-node (already deleted)")
                    else:
                        log.warning(f"Failed to delete site {site['deploy_id']}: {response.status_code}")
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


async def sync_deploy_statuses():
    """Sync deployment statuses from deploy-node to Autosites DB."""
    if not settings.DEPLOY_NODE_URL:
        log.debug("DEPLOY_NODE_URL not configured, skipping deploy status sync")
        return

    log.info("Syncing deployment statuses from deploy-node...")

    try:
        # Get all sites with deploy_id
        sites = await db.list_client_sites(deploy_status=None, limit=1000, offset=0)
        sites_with_deploy = [s for s in sites if s.get('deploy_id')]

        if not sites_with_deploy:
            log.debug("No sites with deploy_id found")
            return

        async with httpx.AsyncClient(timeout=30.0) as client:
            synced_count = 0
            error_count = 0

            for site in sites_with_deploy:
                deploy_id = site['deploy_id']
                try:
                    # Get deployment status from deploy-node
                    response = await client.get(
                        f"{settings.DEPLOY_NODE_URL}/api/deploy/{deploy_id}",
                        timeout=10.0
                    )

                    if response.status_code == 404:
                        log.warning(f"Deployment {deploy_id} not found in deploy-node, marking as failed")
                        await db.update_site_deploy_status(
                            site_id=str(site['id']),
                            deploy_status='failed',
                            error="Deployment not found in deploy-node"
                        )
                        error_count += 1
                        continue

                    if response.status_code != 200:
                        log.warning(f"Failed to get deployment {deploy_id}: {response.status_code}")
                        error_count += 1
                        continue

                    result = response.json()
                    if not result.get('success'):
                        log.warning(f"Deployment {deploy_id} returned error: {result.get('error')}")
                        error_count += 1
                        continue

                    deployment = result.get('data', {})
                    deploy_status_raw = deployment.get('status', '')

                    # Map deploy-node status to Autosites status
                    status_map = {
                        'pending': 'pending',
                        'uploading': 'deploying',
                        'building': 'deploying',
                        'deploying': 'deploying',
                        'completed': 'active',
                        'failed': 'failed',
                        'rollback': 'failed',
                    }
                    deploy_status = status_map.get(deploy_status_raw, deploy_status_raw)

                    # Check if status changed
                    current_status = site.get('deploy_status')
                    if current_status != deploy_status:
                        log.info(
                            f"Status changed for site {site['id']} (deploy {deploy_id}): "
                            f"{current_status} -> {deploy_status}"
                        )

                        # Update site status
                        update_data = {
                            'deploy_status': deploy_status,
                        }

                        # Update preview info if available
                        if deployment.get('preview_slug'):
                            update_data['preview_slug'] = deployment.get('preview_slug')
                            # Construct preview URL
                            preview_slug = deployment.get('preview_slug', '')
                            if preview_slug:
                                # Try to get preview_url from deployment, or construct it
                                preview_url = deployment.get('preview_url')
                                if not preview_url:
                                    # Construct from preview_slug
                                    preview_url = f"https://{preview_slug}.autosites.ru"
                                update_data['preview_url'] = preview_url

                        # Update server info
                        if deployment.get('server_id'):
                            update_data['server_id'] = deployment.get('server_id')
                        if deployment.get('server_name'):
                            update_data['server_name'] = deployment.get('server_name')
                        if deployment.get('server_host'):
                            update_data['server_host'] = deployment.get('server_host')
                        if deployment.get('port'):
                            update_data['container_port'] = deployment.get('port')

                        # Update error if failed
                        if deploy_status == 'failed' and deployment.get('error_message'):
                            update_data['last_error'] = deployment.get('error_message')
                            update_data['last_error_at'] = datetime.now(timezone.utc)

                        # Update domain if set
                        if deployment.get('domain') and deployment.get('domain') != site.get('domain'):
                            update_data['domain'] = deployment.get('domain')
                            update_data['domain_status'] = 'active' if deploy_status == 'active' else 'pending'

                        await db.update_client_site(str(site['id']), update_data)

                        # Update request status if site is now active
                        if deploy_status == 'active' and site.get('request_id'):
                            await db.update_request_status(str(site['request_id']), 'success')

                        synced_count += 1

                except httpx.TimeoutException:
                    log.warning(f"Timeout getting deployment {deploy_id}")
                    error_count += 1
                except Exception as e:
                    log.error(f"Error syncing deployment {deploy_id}: {e}")
                    error_count += 1

            log.info(
                f"Deploy status sync completed: {synced_count} synced, "
                f"{error_count} errors, {len(sites_with_deploy)} total"
            )

    except Exception as e:
        log.error(f"Error in deploy status sync: {e}")


async def run_cron_jobs():
    """Run all cron jobs."""
    try:
        await check_payment_warnings()
        await auto_disable_expired_sites()
        await auto_delete_expired_sites()
        await sync_deploy_statuses()  # Sync deploy statuses
        log.info("Cron jobs completed successfully")
    except Exception as e:
        log.error(f"Error running cron jobs: {e}")


async def deploy_sync_loop():
    """Background loop for deploy status sync (every 5 minutes)."""
    # Wait a bit before first sync
    await asyncio.sleep(30)

    while True:
        try:
            await sync_deploy_statuses()
        except Exception as e:
            log.error(f"Deploy sync error: {e}")
        await asyncio.sleep(300)  # 5 minutes


async def main_cron_loop():
    """Background loop for other cron jobs (every hour)."""
    # Wait a bit before first run
    await asyncio.sleep(60)

    while True:
        try:
            await check_payment_warnings()
            await auto_disable_expired_sites()
            await auto_delete_expired_sites()
        except Exception as e:
            log.error(f"Cron scheduler error: {e}")
        await asyncio.sleep(3600)  # 1 hour


async def start_cron_scheduler():
    """Start cron scheduler (runs every hour, deploy sync every 5 minutes)."""
    # Start both loops as background tasks
    asyncio.create_task(deploy_sync_loop())
    asyncio.create_task(main_cron_loop())

    log.info("Cron scheduler started (deploy sync every 5 min, other jobs every hour)")


if __name__ == "__main__":
    # For testing
    import asyncio
    asyncio.run(run_cron_jobs())

