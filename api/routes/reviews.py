"""
Reviews API - using reviews-digger service

This module provides endpoints for getting reviews from Yandex Maps
using the self-hosted reviews-digger service (browser automation).
"""

import logging
import httpx
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from config import settings
from routes.auth import get_current_user

log = logging.getLogger(__name__)

router = APIRouter()


class ReviewItem(BaseModel):
    author_name: str
    author_avatar: Optional[str] = None
    rating: int  # 1-5 stars
    text: str
    date: Optional[str] = None
    photos: Optional[List[str]] = None


class OrganizationInfo(BaseModel):
    yandex_id: Optional[str] = None
    name: Optional[str] = None
    address: Optional[str] = None
    url: Optional[str] = None


class ReviewsResponse(BaseModel):
    success: bool
    organization: Optional[OrganizationInfo] = None
    business_name: Optional[str] = None  # Alias for organization.name
    business_address: Optional[str] = None  # Alias for organization.address
    average_rating: Optional[float] = None
    total_reviews: Optional[int] = None
    reviews: List[ReviewItem] = []
    source: Optional[str] = None  # "cache" or "scrape"
    error: Optional[str] = None


class ScrapeReviewsRequest(BaseModel):
    """Request to get reviews"""
    query: Optional[str] = None  # Direct query like "Ki-ki nail Ростов"
    yandex_maps_url: Optional[str] = None  # Direct URL (will extract query)
    organization_name: Optional[str] = None  # Name to search for
    city: Optional[str] = None  # City for search
    max_reviews: int = 20  # Maximum number of reviews to fetch


async def get_reviews_from_digger(query: str) -> ReviewsResponse:
    """
    Get reviews using reviews-digger service.

    reviews-digger uses browser automation to scrape Yandex Maps.
    No API keys required.
    """
    if not settings.REVIEWS_DIGGER_URL:
        log.warning("REVIEWS_DIGGER_URL not configured, using mock data")
        return await get_mock_reviews(query)

    try:
        async with httpx.AsyncClient() as client:
            log.info(f"Calling reviews-digger at {settings.REVIEWS_DIGGER_URL}/api/v1/reviews")

            response = await client.post(
                f"{settings.REVIEWS_DIGGER_URL}/api/v1/reviews",
                json={"query": query},
                timeout=120.0  # Browser scraping can take time
            )

            if response.status_code == 404:
                log.warning(f"Organization not found for query: {query}")
                return ReviewsResponse(
                    success=False,
                    error="Организация не найдена"
                )

            if response.status_code != 200:
                log.error(f"Reviews-digger returned {response.status_code}: {response.text}")
                return ReviewsResponse(
                    success=False,
                    error=f"Ошибка сервиса отзывов: {response.status_code}"
                )

            data = response.json()

            # Parse response from reviews-digger
            org = data.get("organization", {})
            reviews_data = data.get("reviews", [])

            reviews = []
            for review in reviews_data:
                reviews.append(ReviewItem(
                    author_name=review.get("author", "Аноним"),
                    rating=review.get("rating", 5),
                    text=review.get("text", ""),
                    date=review.get("date")
                ))

            return ReviewsResponse(
                success=True,
                organization=OrganizationInfo(
                    yandex_id=org.get("yandex_id"),
                    name=org.get("name"),
                    address=org.get("address"),
                    url=org.get("url")
                ),
                business_name=org.get("name"),
                business_address=org.get("address"),
                average_rating=data.get("avg_rating"),
                total_reviews=data.get("total_reviews"),
                reviews=reviews,
                source=data.get("source", "scrape")
            )

    except httpx.TimeoutException:
        log.error("Reviews-digger request timed out")
        return ReviewsResponse(
            success=False,
            error="Превышено время ожидания. Попробуйте позже."
        )
    except httpx.ConnectError as e:
        log.error(f"Failed to connect to reviews-digger: {e}")
        return ReviewsResponse(
            success=False,
            error="Сервис отзывов недоступен"
        )
    except Exception as e:
        log.error(f"Reviews-digger error: {e}", exc_info=True)
        return ReviewsResponse(
            success=False,
            error=str(e)
        )


async def get_mock_reviews(query: str) -> ReviewsResponse:
    """
    Return mock reviews for testing when reviews-digger is not configured.
    """
    return ReviewsResponse(
        success=True,
        organization=OrganizationInfo(
            name=query,
            address="г. Москва, ул. Примерная, д. 1"
        ),
        business_name=query,
        business_address="г. Москва, ул. Примерная, д. 1",
        average_rating=4.7,
        total_reviews=156,
        reviews=[
            ReviewItem(
                author_name="Александр М.",
                rating=5,
                text="Отличный сервис! Быстро и качественно выполнили работу. Рекомендую всем!",
                date="2024-12-15"
            ),
            ReviewItem(
                author_name="Мария К.",
                rating=5,
                text="Очень довольна результатом. Профессиональный подход, приятные цены.",
                date="2024-12-10"
            ),
            ReviewItem(
                author_name="Дмитрий С.",
                rating=4,
                text="Хорошая компания, всё сделали в срок. Единственное - хотелось бы больше вариантов.",
                date="2024-12-05"
            ),
            ReviewItem(
                author_name="Елена В.",
                rating=5,
                text="Работаю с ними уже второй год. Всегда на высоте!",
                date="2024-11-28"
            ),
            ReviewItem(
                author_name="Игорь П.",
                rating=4,
                text="Достойное качество за свои деньги. Буду обращаться ещё.",
                date="2024-11-20"
            ),
        ],
        source="mock"
    )


def build_query(organization_name: str = None, city: str = None, yandex_maps_url: str = None) -> str:
    """Build search query from parameters."""
    if yandex_maps_url:
        # Extract org name from URL if possible, otherwise use URL as query
        # URL format: https://yandex.ru/maps/org/company_name/1234567890/
        try:
            parts = yandex_maps_url.split("/org/")
            if len(parts) > 1:
                org_part = parts[1].split("/")[0]
                # URL-decoded name
                return org_part.replace("_", " ")
        except:
            pass
        return yandex_maps_url

    if organization_name:
        query = organization_name
        if city:
            query += f", {city}"
        return query

    return ""


@router.post("/scrape", response_model=ReviewsResponse)
async def scrape_reviews(
    request: ScrapeReviewsRequest,
    user: dict = Depends(get_current_user)
):
    """
    Get reviews from Yandex Maps.

    You can provide either:
    - query: Direct search query like "Ki-ki nail Ростов"
    - organization_name + city: Will be combined into query
    - yandex_maps_url: URL will be parsed to extract organization

    Returns a list of reviews with author name, rating, text, and date.
    """
    # Build query from available parameters
    query = request.query
    if not query:
        query = build_query(
            organization_name=request.organization_name,
            city=request.city,
            yandex_maps_url=request.yandex_maps_url
        )

    if not query:
        raise HTTPException(
            status_code=400,
            detail="Укажите query, organization_name или yandex_maps_url"
        )

    log.info(f"Getting reviews for query: {query}")

    result = await get_reviews_from_digger(query)

    if not result.success:
        log.warning(f"Review fetch failed: {result.error}")
    else:
        log.info(f"Successfully got {len(result.reviews)} reviews")

    return result


@router.get("/by-url", response_model=ReviewsResponse)
async def get_reviews_by_url(
    url: str = Query(..., description="Yandex Maps URL"),
    max_reviews: int = Query(20, ge=1, le=100),
    user: dict = Depends(get_current_user)
):
    """
    Get reviews by Yandex Maps URL.

    Example URL: https://yandex.ru/maps/org/company_name/1234567890/
    """
    query = build_query(yandex_maps_url=url)
    return await get_reviews_from_digger(query)


@router.get("/search", response_model=ReviewsResponse)
async def search_reviews(
    name: str = Query(..., description="Organization name"),
    city: Optional[str] = Query(None, description="City name"),
    max_reviews: int = Query(20, ge=1, le=100),
    user: dict = Depends(get_current_user)
):
    """
    Search for organization and get its reviews.
    """
    query = build_query(organization_name=name, city=city)
    return await get_reviews_from_digger(query)


# Internal function for use in other routes (e.g., generate_site)
async def fetch_reviews_for_generation(company_name: str, address: str = None) -> dict:
    """
    Fetch reviews for site generation.
    Returns dict with reviews data suitable for n8n payload.

    This is called internally before sending request to n8n.
    """
    query = company_name
    if address:
        query += f", {address}"

    log.info(f"Fetching reviews for generation: {query}")

    result = await get_reviews_from_digger(query)

    if not result.success:
        log.warning(f"Failed to fetch reviews for generation: {result.error}")
        return {
            "success": False,
            "error": result.error,
            "reviews": []
        }

    # Format reviews for n8n
    reviews_for_n8n = []
    for review in result.reviews:
        reviews_for_n8n.append({
            "author": review.author_name,
            "text": review.text,
            "rating": review.rating,
            "date": review.date
        })

    return {
        "success": True,
        "organization": {
            "yandex_id": result.organization.yandex_id if result.organization else None,
            "name": result.organization.name if result.organization else None,
            "address": result.organization.address if result.organization else None,
            "url": result.organization.url if result.organization else None
        },
        "reviews": reviews_for_n8n,
        "total_reviews": result.total_reviews,
        "avg_rating": result.average_rating,
        "source": result.source
    }
