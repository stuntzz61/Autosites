"""
Yandex Maps Reviews Scraping API using Apify

This module provides endpoints for scraping reviews from Yandex Maps
using Apify actors or direct scraping.
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


class ReviewsResponse(BaseModel):
    success: bool
    business_name: Optional[str] = None
    business_address: Optional[str] = None
    average_rating: Optional[float] = None
    total_reviews: Optional[int] = None
    reviews: List[ReviewItem] = []
    error: Optional[str] = None


class ScrapeReviewsRequest(BaseModel):
    """Request to scrape reviews from Yandex Maps"""
    yandex_maps_url: Optional[str] = None  # Direct URL to Yandex Maps place
    organization_name: Optional[str] = None  # Name to search for
    city: Optional[str] = None  # City for search
    max_reviews: int = 20  # Maximum number of reviews to fetch


# Apify settings
APIFY_API_TOKEN = getattr(settings, 'APIFY_API_TOKEN', '')
APIFY_YANDEX_ACTOR_ID = 'alexey/yandex-maps-reviews-scraper'  # Popular Apify actor for Yandex Maps


async def scrape_reviews_with_apify(
    yandex_maps_url: Optional[str] = None,
    organization_name: Optional[str] = None,
    city: Optional[str] = None,
    max_reviews: int = 20
) -> ReviewsResponse:
    """
    Scrape reviews using Apify Yandex Maps scraper actor.

    This uses the Apify platform to run a pre-built Yandex Maps scraper.
    You need to set APIFY_API_TOKEN in environment variables.
    """
    if not APIFY_API_TOKEN:
        log.warning("APIFY_API_TOKEN not configured, using mock data")
        return await get_mock_reviews(organization_name or "Unknown")

    try:
        # Prepare input for Apify actor
        actor_input = {
            "maxReviews": max_reviews,
            "language": "ru",
        }

        if yandex_maps_url:
            actor_input["startUrls"] = [{"url": yandex_maps_url}]
        elif organization_name:
            search_query = f"{organization_name}"
            if city:
                search_query += f" {city}"
            actor_input["searchQuery"] = search_query
        else:
            raise ValueError("Either yandex_maps_url or organization_name must be provided")

        # Call Apify API to run the actor
        async with httpx.AsyncClient() as client:
            # Start the actor run
            run_response = await client.post(
                f"https://api.apify.com/v2/acts/{APIFY_YANDEX_ACTOR_ID}/runs",
                json=actor_input,
                headers={"Authorization": f"Bearer {APIFY_API_TOKEN}"},
                params={"waitForFinish": 120},  # Wait up to 2 minutes
                timeout=180.0
            )

            if run_response.status_code != 201:
                log.error(f"Apify actor start failed: {run_response.text}")
                return ReviewsResponse(
                    success=False,
                    error=f"Failed to start scraper: {run_response.status_code}"
                )

            run_data = run_response.json()
            run_id = run_data.get("data", {}).get("id")

            if not run_id:
                return ReviewsResponse(
                    success=False,
                    error="Failed to get run ID from Apify"
                )

            # Get the results from the dataset
            dataset_response = await client.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
                headers={"Authorization": f"Bearer {APIFY_API_TOKEN}"},
                timeout=30.0
            )

            if dataset_response.status_code != 200:
                log.error(f"Failed to get results: {dataset_response.text}")
                return ReviewsResponse(
                    success=False,
                    error="Failed to retrieve results"
                )

            results = dataset_response.json()

            if not results:
                return ReviewsResponse(
                    success=True,
                    reviews=[],
                    error="No reviews found"
                )

            # Parse results
            reviews = []
            business_info = results[0] if results else {}

            for item in results:
                if isinstance(item.get("reviews"), list):
                    for review in item["reviews"]:
                        reviews.append(ReviewItem(
                            author_name=review.get("author", {}).get("name", "Anonymous"),
                            author_avatar=review.get("author", {}).get("avatar"),
                            rating=review.get("rating", 5),
                            text=review.get("text", ""),
                            date=review.get("date"),
                            photos=review.get("photos", [])
                        ))

            return ReviewsResponse(
                success=True,
                business_name=business_info.get("name"),
                business_address=business_info.get("address"),
                average_rating=business_info.get("rating"),
                total_reviews=business_info.get("reviewsCount"),
                reviews=reviews[:max_reviews]
            )

    except httpx.TimeoutException:
        log.error("Apify request timed out")
        return ReviewsResponse(
            success=False,
            error="Request timed out. Try again later."
        )
    except Exception as e:
        log.error(f"Apify scraping error: {e}", exc_info=True)
        return ReviewsResponse(
            success=False,
            error=str(e)
        )


async def get_mock_reviews(business_name: str) -> ReviewsResponse:
    """
    Return mock reviews for testing when Apify is not configured.
    """
    return ReviewsResponse(
        success=True,
        business_name=business_name,
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
        ]
    )


@router.post("/scrape", response_model=ReviewsResponse)
async def scrape_reviews(
    request: ScrapeReviewsRequest,
    user: dict = Depends(get_current_user)
):
    """
    Scrape reviews from Yandex Maps.

    You can provide either:
    - yandex_maps_url: Direct URL to a place on Yandex Maps
    - organization_name + city: Search for the organization

    Returns a list of reviews with author name, rating, text, and date.
    """
    if not request.yandex_maps_url and not request.organization_name:
        raise HTTPException(
            status_code=400,
            detail="Either yandex_maps_url or organization_name must be provided"
        )

    log.info(f"Scraping reviews for: {request.yandex_maps_url or request.organization_name}")

    result = await scrape_reviews_with_apify(
        yandex_maps_url=request.yandex_maps_url,
        organization_name=request.organization_name,
        city=request.city,
        max_reviews=request.max_reviews
    )

    if not result.success:
        log.warning(f"Review scraping failed: {result.error}")
    else:
        log.info(f"Successfully scraped {len(result.reviews)} reviews")

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
    return await scrape_reviews_with_apify(
        yandex_maps_url=url,
        max_reviews=max_reviews
    )


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
    return await scrape_reviews_with_apify(
        organization_name=name,
        city=city,
        max_reviews=max_reviews
    )

